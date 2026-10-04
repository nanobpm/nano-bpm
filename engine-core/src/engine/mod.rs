//! The engine: a single-writer command/event/applier loop.
//!
//! [`Engine::apply_command`] is the one entry point that changes anything. It:
//!
//! 1. validates the [`Command`] and emits the top-level event(s),
//! 2. drives an internal work queue of [`Step`]s — the BPMN element lifecycle —
//!    until the instance is quiescent (finished, or resting on a job/incident),
//! 3. detects process-instance completion,
//!
//! applying every event through [`state::apply`] as it goes. The processor
//! ([`Engine::process_step`]) only *reads* state and *decides*; it never mutates.

use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use crate::command::Command;
use crate::event::Event;
use crate::model::{ElementId, ElementKind, IncomingFlow, ProcessDefinition, SequenceFlow, Value};
use crate::state::{self, Key, ProcessInstanceState, State};

mod agent_behavior;
mod api;
mod boundary;
mod call_activity;
mod debug;
mod memory;
mod resolve;

pub use debug::{BreakCondition, DebugSession};

/// Whether the fixpoint loop should keep draining after a processed step, or pause
/// and hand control back to a debugger. See [`StepDriver`].
enum Drive {
    Continue,
    Pause,
}

/// Consulted by [`Engine::run_with`] after every processed [`Step`], with the
/// slice of events that step emitted. Production uses [`RunToCompletion`] (never
/// pauses); the debugger supplies pausing drivers so the *same* loop can single-
/// step or break, without forking the step semantics in [`Engine::process_step`].
trait StepDriver {
    fn after_step(&mut self, events: &[Event]) -> Drive;
}

/// The run-to-completion driver: never pauses. This is what real workers see;
/// because `run_with` is generic over the driver, the `RunToCompletion`
/// instantiation monomorphizes and its `#[inline]` `after_step` (always
/// `Continue`) optimizes away, so the production path keeps the old `run` loop's
/// codegen — no vtable, no per-step call.
struct RunToCompletion;

impl StepDriver for RunToCompletion {
    #[inline]
    fn after_step(&mut self, _events: &[Event]) -> Drive {
        Drive::Continue
    }
}

/// The captured mid-drain state of the fixpoint loop: the still-pending work queue
/// and the conditional-reevaluation cursor. Returned by [`Engine::run_with`] when a
/// driver pauses, and fed back in to resume exactly where it left off. Opaque to
/// callers (wraps the private [`Step`] queue), owned by a [`DebugSession`].
struct Paused {
    queue: VecDeque<Step>,
    cursor: usize,
}

/// The version of the serialized snapshot/event **payload** shape (the
/// serde-derived form of [`EngineSnapshot`] / [`State`] / [`crate::Event`]).
///
/// This is the single source of truth the on-disk snapshot envelope stamps into
/// its header (`format_version`) and the number the CI drift guard (#1069) is
/// tied to. It lives here in `engine-core` — alongside the types it versions —
/// rather than in the storage crate, because the dependency edge is one-way
/// (`nano-server-storage` -> `engine-core`); the storage envelope references
/// this constant *downward*. engine-core cannot see storage, so the guard that
/// fingerprints these types (also in engine-core) can only tie to a constant
/// that lives here.
///
/// Bump this whenever a **breaking** change is made to the serialized shape of
/// the snapshot or event payload (a rename/retag/type-change/reorder — anything
/// serde's additive `#[serde(default)]` forward-compat does not rescue). A bump
/// declares "old on-disk snapshots at a lower version are not payload-compatible
/// with this build", which the loader surfaces as a typed format mismatch (and
/// #1071's replay-migrator branches on).
///
/// `0` is reserved for the historical *headerless* on-disk format (bare
/// `serde_json` with no envelope); the first versioned envelope is `1`.
/// Version `2` writes opaque string lease tokens, typed system-prompt arrays,
/// and explicitly nullable history metrics.
/// Persistence decoders retain numeric tokens and literal legacy prompt strings
/// so version-1 journals and snapshots still load.
pub const SNAPSHOT_FORMAT_VERSION: u32 = 2;
/// A compact, serializable capture of an [`Engine`]: its materialized [`State`]
/// plus the scalar generator and clock metadata required to resume operation
/// identically. Produced by [`Engine::snapshot`] and consumed by
/// [`Engine::from_snapshot`]; the body of a bounded Raft state-machine snapshot.
#[cfg(feature = "serde")]
#[derive(Clone, Debug, serde::Serialize, serde::Deserialize)]
pub struct EngineSnapshot {
    pub state: State,
    pub partition_id: u64,
    pub next_local: u64,
    pub num_partitions: u64,
    pub now: u64,
    pub start_dispatch_rr: u64,
}

/// Synthetic worker name stamped on a job whose activation lease was recovered
/// from a soft lease digest by a newly-promoted leader (see
/// [`Engine::recover_lease`]). It marks the lease as "restored from a digest, not
/// held by a live worker connection".
pub const LEASE_DIGEST_WORKER: &str = "__lease_digest__";

struct ActivationPlan {
    job_keys: Vec<Key>,
    worker: String,
    timeout: u64,
    now: u64,
    options: crate::JobActivationOptions,
    durable: bool,
}

/// An embeddable BPMN engine instance.
///
/// Holds all state in memory. It is `Send` and contains no threads, locks or I/O,
/// so it can be owned by a single actor/task on a server, wrapped behind an FFI
/// boundary on mobile, or compiled to wasm.
#[derive(Debug, Default)]
pub struct Engine {
    state: State,
    /// Id of the partition this engine instance owns. Mints keys in its own
    /// namespace (see [`crate::partition_of`]); `0` for a single-partition host,
    /// which yields the historical `1, 2, 3, …` key sequence.
    partition_id: u64,
    /// Per-partition monotonic local counter. The next minted key is
    /// `compose_key(partition_id, next_local + 1)`. The single-writer loop makes
    /// plain increments deterministic.
    next_local: u64,
    /// Total number of partitions in the cluster (`1` for a single-partition
    /// host). Used to place message subscriptions on the partition owning
    /// `hash(correlation_key)` (see [`crate::subscription_partition`]); with a
    /// value of `1` every subscription is local, so behaviour is unchanged.
    num_partitions: u64,
    /// The clock reading for the command currently being processed, in the units
    /// the host supplies (Unix epoch milliseconds on the server). Set at the top
    /// of [`Engine::apply_command_at`] and read where the engine stamps a
    /// timestamp onto an event (e.g. when an incident is raised). The engine
    /// never reads a wall clock itself; replay is unaffected because the
    /// timestamp is carried on the event.
    now: u64,
    /// Round-robin cursor for spreading start-triggered (message-/timer-start)
    /// instances across the cluster's partitions. Not journaled — at RF=1 it
    /// only balances load (the chosen target is baked onto the emitted
    /// `StartInstanceDispatched` event, so replay is unaffected); a restart
    /// simply resumes the rotation from zero.
    start_dispatch_rr: u64,
    /// When `true`, a `CompleteJob` / `FailJob` / `ThrowJobError` is accepted for a
    /// job that has never been activated (the `activated` latch is not required).
    /// This supports a clustered mode where the activation lock is *leader-local*
    /// (not replicated through Raft): a follower's engine never observes the
    /// `JobActivated` event, so a replicated completion would otherwise fail the
    /// `JobNotActivated` check and diverge from the leader. Possession of the job
    /// key *is* the capability (keys are only handed out by activation), so this
    /// stays within the at-least-once contract. Runtime config (set identically on
    /// every replica from `NANOBPMN_REPLICATE_ACTIVATION`); NOT part of the
    /// snapshot, so it never affects replay/snapshot determinism. Defaults to
    /// `false` — the strict single-node/RF=1 behaviour is unchanged.
    lenient_completion: bool,
    /// When `true`, runtime event application ([`Engine::emit`]) records the
    /// instances whose variables changed into [`Engine::dirty_vars`] and terminal
    /// evictions into [`Engine::forgotten_vars`], so a host can checkpoint just
    /// the delta into an authoritative durable variable store and write a lean
    /// (control-only) snapshot. Off by default (the full-variable snapshot path is
    /// unchanged); set by the host from `NANOBPMN_LEAN_SNAPSHOT`. Not part of the
    /// snapshot — it is pure host-side bookkeeping and never affects determinism.
    track_dirty_vars: bool,
    /// Instances whose process-level variables changed since the last
    /// [`drain_dirty_vars`](Engine::drain_dirty_vars) (i.e. since the last
    /// snapshot checkpoint). Only populated when [`track_dirty_vars`] is set.
    dirty_vars: HashSet<Key>,
    /// Instances evicted (terminal) since the last checkpoint, whose durable
    /// variable rows must be deleted from the store. Only populated when
    /// [`track_dirty_vars`] is set.
    forgotten_vars: HashSet<Key>,
    /// Follower-only safety net for the retirement digest (RF>1). A leader completes
    /// and exporter-evicts an instance and broadcasts its key, but under leader-durable
    /// the ack happens on the leader before the async learner has applied the
    /// instance's `CreateInstance`, so [`Engine::retire_instances`] finds the key
    /// absent. Rather than drop the retirement (and leak the instance once its create
    /// lands), the absent key is remembered here; the next `apply_command_at` that
    /// materializes it retires it immediately. Populated only on a follower replica
    /// (a leader never receives a retirement digest, so this stays empty and the
    /// per-command check is free). Bounded — it only ever holds keys in the brief
    /// window between a retirement and its create, so it drains continuously. NOT
    /// part of the snapshot; pure host-side bookkeeping, never affects determinism.
    retired_tombstones: HashSet<Key>,
    /// Sub-process scopes torn down (by a scoped terminate end or an interrupting
    /// boundary) during the command currently draining. A scoped teardown removes
    /// every descendant token but deliberately leaves the enclosing sub-process
    /// token active until the post-drain [`complete_drained_subprocesses`](Self::complete_drained_subprocesses)
    /// sweep completes it — so a sibling branch's `Step::Activate` still queued in
    /// the *same* drain would otherwise pass [`step_targets_dead_scope`](Self::step_targets_dead_scope)'s
    /// "scope still active" check and recreate a token inside the dead scope,
    /// which then keeps the parent from ever draining (a wedge). Recording the
    /// scope here lets the guard reject those queued activations. NOT journaled:
    /// the guard runs only during live command processing (recovery replays the
    /// emitted events, never re-runs the guard), and the whole window lives inside
    /// one command's drain — so it is cleared at the top of every
    /// [`plan_command_at`](Self::plan_command_at) and never affects determinism.
    torn_down_scopes: HashSet<Key>,
    /// Host-injected **cluster variables** (see [`crate::cluster_vars`]): a shared,
    /// mutable snapshot of global + per-tenant configuration values that FEEL
    /// expressions resolve at runtime. External configuration, not journaled
    /// state: it is never part of a snapshot and never affects replay determinism
    /// (the host re-installs the shared handle on every engine rebuild via
    /// [`set_cluster_variables`](Engine::set_cluster_variables)). Empty by default,
    /// in which case variable resolution keeps its zero-copy fast path.
    cluster_variables: crate::cluster_vars::ClusterVariables,
}

/// A `zeebe:ioMapping` source expression that failed to evaluate. Carries a
/// human-readable `reason` for the `IO_MAPPING_ERROR` incident the caller raises
/// (#939); the `reason` text embeds the failing mapping's `source` expression and
/// `target`. Discrimination of an *expected* body-level per-child-binding failure
/// (`loopCounter` / the configured `inputElement`, absent at the body level) from
/// a *genuine* failure is done inside [`Engine::eval_io_mappings_tolerating`] by
/// re-checking the mapping's referenced variables — not by the caller reading a
/// field off this struct (#946).
pub(crate) struct IoMappingFailure {
    pub(crate) reason: String,
}

/// The instance-derived context copied off an [`AgentInstance`](crate::agent::AgentInstance)
/// when materialising a batch of AgentHistory turns, so the borrow is released
/// before keys are minted / events emitted.
struct AgentHistoryBase {
    instance_key: Key,
    element_instance_key: Key,
    process_instance_key: Key,
    root_process_instance_key: Key,
    bpmn_process_id: String,
    process_definition_key: Key,
    tenant_id: String,
}

/// The decision an ad-hoc container makes after a tool's output mappings are
/// applied, driven by its `<completionCondition>` (ADR 0023 seam 4). Returned by
/// [`Engine::adhoc_completion_after_tool`] so the leaf path
/// (`complete_adhoc_tool`) and the mid-chain hand-off (`continue_adhoc_inner_flow`)
/// share one canonical completion decision.
enum AdHocPostTool {
    /// The completion condition triggered — finish the container now. `cancel`
    /// mirrors `cancelRemainingInstances`: `true` cancels any still-active tools,
    /// `false` completes only because none remain.
    Complete { cancel: bool },
    /// `cancelRemainingInstances=false` with other children still active — latch
    /// the fulfilment (emitting `fulfilled_event` the first time) and park so the
    /// container completes as its children drain, without a new agent turn or any
    /// further activation.
    Defer { fulfilled_event: Option<Box<Event>> },
    /// The completion condition did not trigger — continue normally.
    Continue,
}

/// The two synchronising joins: a parallel or inclusive gateway with more than
/// one incoming flow ([`Engine::activate_join`]).
#[derive(Clone, Copy)]
enum JoinKind {
    Parallel,
    Inclusive,
}

/// A unit of internal work in the processing loop — one transition of the BPMN
/// element lifecycle.
enum Step {
    /// Run an element through `ACTIVATING -> ACTIVATED` (and decide what comes
    /// next based on its kind).
    Activate {
        instance_key: Key,
        element_id: String,
        /// The enclosing sub-process element instance this activation runs in,
        /// or `0` for the process-level (root) scope.
        scope: Key,
        /// The sequence flow this token arrived over, or `None` when the
        /// activation did not come over a flow (a start event, a boundary event,
        /// a link catch, a modification, ...). A parallel join counts arrivals
        /// per flow (#1233). Build flow activations with
        /// [`Engine::take_flow`], never by hand.
        via: Option<IncomingFlow>,
    },
    /// Run an already-activated element through `COMPLETING -> COMPLETED` and take
    /// its outgoing sequence flows.
    Complete {
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    },
    /// Complete an ad-hoc call-activity tool child once its spawned child process
    /// finished (issue #1159). Carries the tool's output projection ALREADY
    /// evaluated once (in `complete_adhoc_call_activity_tool`) against the child
    /// process's produced variables, so the shared `complete_adhoc_tool` leaf
    /// applies it verbatim instead of re-evaluating it against the seeded tool
    /// scope — which would double-apply chained mappings (`status -> intermediate`
    /// then `intermediate -> toolCallResult`) and break `eval_io_mappings_in`'s
    /// single-pass semantics. The bridge's seed events are applied before this
    /// step runs, so the container's `outputElement` still sees the real result.
    CompleteAdHocCallActivityTool {
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        container_key: Key,
        inner_key: Key,
        output_updates: HashMap<String, Value>,
    },
    /// Create a fresh job for an already-active service-task element instance.
    /// Used to retry a parked service task when its incident is resolved (the
    /// element instance stays active throughout; only a new job is minted).
    CreateJob {
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    },
    /// Re-run the activation body of an already-active message intermediate
    /// catch event to re-open its subscription. Used to retry a catch whose
    /// correlation-key expression raised an `ExpressionEvaluation` incident at
    /// open time: the element instance stays active throughout, so its
    /// `ElementActivating`/`ElementActivated` are not re-emitted — only the
    /// subscription is re-derived once the referenced variables are corrected.
    ReopenCatch {
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    },
    /// Re-run the activation body of an already-active element whose input
    /// `zeebe:ioMapping` raised an `IoMapping` incident (#939). The element
    /// instance is already ACTIVATED (its `ElementActivating`/`ElementActivated`
    /// were emitted on the first pass and are not re-emitted); this re-applies the
    /// (now-fixed) input mappings and re-enacts the element's behaviour, exactly
    /// as the first activation would have had the mappings evaluated cleanly.
    RetryActivation {
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    },
    /// Activate one child of a multi-instance body: instantiate `element_id`
    /// again in the body's scope with the `index`-th item's local bindings. The
    /// item and bindings are read from the body's runtime state.
    ActivateMiChild {
        instance_key: Key,
        element_id: String,
        body_key: Key,
        index: usize,
    },
    /// Complete a multi-instance body: aggregate the collected output, cancel any
    /// children still running (an early completion-condition fire), write the
    /// output collection, and take the activity's outgoing flow.
    CompleteMiBody { instance_key: Key, body_key: Key },
    /// Activate one ad-hoc "tool" child: instantiate `element_id` inside the
    /// container's scope, seeded with the agent's `variables`, and mark it active
    /// in the container. Created for each activate-element instruction an ad-hoc
    /// agent job returns (ADR 0023 seam 2).
    ActivateAdHocTool {
        instance_key: Key,
        container_key: Key,
        element_id: String,
        variables: HashMap<String, Value>,
    },
    /// Complete an ad-hoc container: write its aggregated `outputCollection`,
    /// (when `cancel`) cancel any tool children still running, drop its runtime
    /// state and take the container's outgoing flow. Driven by the agent
    /// signalling completion / cancellation, or by the loop draining with no
    /// further tools requested.
    CompleteAdHoc {
        instance_key: Key,
        container_key: Key,
        cancel: bool,
    },
    /// Advance an element's execution-listener chain after one listener job
    /// completed (ADR 0037): create the next listener job, or — when the chain
    /// is drained — run the deferred lifecycle transition (a `Start` chain runs
    /// the element's normal activation behaviour; an `End` chain emits
    /// `ElementCompleted` and takes the outgoing flows).
    AdvanceListener {
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        event_type: crate::model::ListenerEventType,
        /// The 0-based index of the listener that just completed.
        index: usize,
        /// The element's enclosing flow scope.
        scope: Key,
    },
    /// Advance a user task's task-listener chain after a listener job completed
    /// (ADR 0037 §6): mint the next listener's job, or — when the chain drains —
    /// commit the deferred transition (assign/update/complete/create/cancel).
    AdvanceTaskListener {
        user_task_key: Key,
        event_type: crate::model::TaskListenerEventType,
        /// The 0-based index (within this event type's listener list) of the
        /// listener that just completed.
        index: usize,
    },
    /// Complete a call-activity token once its spawned child process instance
    /// has finished. The parent's call-activity element instance parked in
    /// ACTIVATED while the child ran; this projects the child's final variables
    /// through the call activity's output mappings, completes the element and
    /// takes its outgoing flow. `child_variables` is captured at the moment the
    /// child completes (before the terminal instance drops its variables).
    CompleteCallActivity {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        child_variables: HashMap<String, Value>,
    },
    /// Re-drive a multi-instance **child**'s activation after its input-mapping
    /// incident is resolved (#946): re-apply the child's inputs against its
    /// already-bound `inputElement`/`loopCounter` scope and re-enact its
    /// behaviour, for the same already-activated child instance (its activation
    /// events are not re-emitted). See [`Engine::retry_mi_child_activation`].
    RetryMiChildActivation {
        instance_key: Key,
        element_id: ElementId,
        body_key: Key,
        child_key: Key,
        index: usize,
    },
    /// Re-drive a multi-instance **body**'s activation (fan-out) after its body
    /// input-mapping incident is resolved (#946): re-apply the body input
    /// mappings, re-evaluate the input collection and spawn the children, for the
    /// same already-activated body (its activation/scope events are not
    /// re-emitted). See [`Engine::run_mi_body_activation`].
    RetryMiBodyActivation {
        instance_key: Key,
        element_id: ElementId,
        body_key: Key,
        scope: Key,
    },
    /// Re-drive a call activity's child-process **spawn** after its input-mapping
    /// incident is resolved (#946): re-attempt seeding and creating the child
    /// process for the already-activated call activity (its boundary events were
    /// armed on the first pass and are not re-armed). `preserved_seed` carries the
    /// ad-hoc call-activity *tool*'s single-pass input projection captured on the
    /// failed first spawn (#1176) so the respawn reuses it verbatim rather than
    /// re-projecting the tool's (possibly chained) input mappings against the
    /// already-mutated child scope; `None` for a mainstream call activity (whose
    /// seed re-derives idempotently from its still-stable element scope) and for
    /// legacy journals predating #1176.
    RetryCallActivitySpawn {
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        preserved_seed: Option<HashMap<String, Value>>,
    },
}

/// Maximum depth of the call-activity parent-child chain before a spawning call
/// activity parks on an incident instead of recursing further. Guards against
/// self / mutually-recursive callees that never terminate.
const MAX_CALL_ACTIVITY_DEPTH: usize = 1000;

impl Engine {
    /// Creates an empty engine for the single-partition (id `0`) namespace.
    pub fn new() -> Self {
        Self::with_partition(0)
    }

    /// Creates an empty engine that mints keys in partition `partition_id`'s
    /// namespace. Every key it produces carries `partition_id` in its high bits
    /// (see [`crate::compose_key`]), so keys are globally unique across a set of
    /// partitions and route back to their owner via [`crate::partition_of`].
    ///
    /// Panics if `partition_id` exceeds [`crate::MAX_PARTITION_ID`].
    pub fn with_partition(partition_id: u64) -> Self {
        assert!(
            partition_id <= state::MAX_PARTITION_ID,
            "partition id {partition_id} exceeds MAX_PARTITION_ID {}",
            state::MAX_PARTITION_ID
        );
        Self {
            state: State::new(),
            partition_id,
            next_local: 0,
            num_partitions: 1,
            now: 0,
            start_dispatch_rr: 0,
            lenient_completion: false,
            track_dirty_vars: false,
            dirty_vars: HashSet::new(),
            forgotten_vars: HashSet::new(),
            retired_tombstones: HashSet::new(),
            torn_down_scopes: HashSet::new(),
            cluster_variables: crate::cluster_vars::ClusterVariables::default(),
        }
    }

    /// Read-only access to the full engine state (useful for queries and tests).
    pub fn state(&self) -> &State {
        &self.state
    }

    /// Sets the cluster-wide partition count used to place message
    /// subscriptions (see [`crate::subscription_partition`]). Defaults to `1`
    /// (single-partition, every subscription local). A clustered host calls
    /// this with `NANOBPMN_PARTITIONS` right after construction/replay so the
    /// engine routes catch-event subscriptions to the partition owning their
    /// correlation key. Must be identical on every node and stable for the life
    /// of the cluster.
    pub fn set_num_partitions(&mut self, num_partitions: u64) {
        self.num_partitions = num_partitions.max(1);
    }

    /// Enables (or disables) lenient completion: when `true`, `CompleteJob` /
    /// `FailJob` / `ThrowJobError` no longer require the job to have been activated
    /// first. A clustered host sets this from `NANOBPMN_REPLICATE_ACTIVATION=0` so
    /// the activation lock can stay leader-local (un-replicated) while replicated
    /// completions still apply cleanly on followers that never saw the activation.
    /// See the `lenient_completion` field. Must be set identically on every replica.
    pub fn set_lenient_completion(&mut self, lenient: bool) {
        self.lenient_completion = lenient;
    }

    /// Whether lenient completion is enabled (see [`Self::set_lenient_completion`]).
    pub fn lenient_completion(&self) -> bool {
        self.lenient_completion
    }

    /// Installs the shared [`ClusterVariables`](crate::cluster_vars::ClusterVariables)
    /// handle the engine reads while assembling a FEEL evaluation context. The host
    /// (the gateway) owns the write side and mutates the snapshot as REST create /
    /// update / delete requests land; the engine observes the live set. Cluster
    /// variables are external configuration, so this is host-side wiring that never
    /// affects replay/snapshot determinism — the host re-installs the handle after
    /// every engine rebuild (replay / snapshot restore) so the link survives.
    pub fn set_cluster_variables(
        &mut self,
        cluster_variables: crate::cluster_vars::ClusterVariables,
    ) {
        self.cluster_variables = cluster_variables;
    }

    /// The shared cluster-variable handle (see [`Self::set_cluster_variables`]).
    pub fn cluster_variables(&self) -> &crate::cluster_vars::ClusterVariables {
        &self.cluster_variables
    }

    /// The cluster-wide partition count this engine is configured with.
    pub fn num_partitions(&self) -> u64 {
        self.num_partitions
    }

    /// The partition that owns the message subscription for `correlation_key`.
    /// Always this engine's own partition when single-partition.
    pub fn subscription_partition(&self, correlation_key: &str) -> u64 {
        state::subscription_partition(correlation_key, self.num_partitions)
    }

    /// Rebuilds an engine by replaying a recorded event log.
    ///
    /// State is reconstructed through the applier ([`state::apply`]) — the same
    /// path used at runtime — and the key generator is advanced past every key
    /// the log assigned, so commands applied after recovery never mint a key
    /// that collides with a replayed one. The clock resets to `0`; timestamps
    /// are carried on the events themselves, so history (e.g. incident
    /// `created_at`) is preserved, and the next command sets the clock again.
    ///
    /// This is the durability primitive: a host persists the events returned by
    /// [`Engine::apply_command_at`] and, on restart, feeds them back here.
    pub fn replay<I>(events: I) -> Self
    where
        I: IntoIterator<Item = Event>,
    {
        Self::replay_partition(0, events)
    }

    /// Like [`Engine::replay`] but reconstructs partition `partition_id`. The key
    /// generator is advanced past every replayed key **that belongs to this
    /// partition** (`partition_of(key) == partition_id`); keys minted by other
    /// partitions (e.g. a deployment replicated in from partition 0) are applied
    /// to state but never advance this partition's local counter, so it keeps
    /// minting in its own namespace without colliding.
    pub fn replay_partition<I>(partition_id: u64, events: I) -> Self
    where
        I: IntoIterator<Item = Event>,
    {
        assert!(
            partition_id <= state::MAX_PARTITION_ID,
            "partition id {partition_id} exceeds MAX_PARTITION_ID {}",
            state::MAX_PARTITION_ID
        );
        let mut state = State::new();
        let mut next_local: u64 = 0;
        for event in events {
            let max_key = event.max_key();
            if state::partition_of(max_key) == partition_id {
                next_local = next_local.max(state::local_of(max_key));
            }
            state::apply(&mut state, &event);
        }
        Self {
            state,
            partition_id,
            next_local,
            num_partitions: 1,
            now: 0,
            start_dispatch_rr: 0,
            lenient_completion: false,
            track_dirty_vars: false,
            dirty_vars: HashSet::new(),
            forgotten_vars: HashSet::new(),
            retired_tombstones: HashSet::new(),
            torn_down_scopes: HashSet::new(),
            cluster_variables: crate::cluster_vars::ClusterVariables::default(),
        }
    }

    fn mint_key(&mut self) -> Key {
        self.next_local += 1;
        debug_assert!(
            self.next_local <= state::LOCAL_MASK,
            "partition {} exhausted its 51-bit local key space",
            self.partition_id
        );
        state::compose_key(self.partition_id, self.next_local)
    }

    /// The root (top-level ancestor) process instance key for `instance_key`:
    /// walks up the call-activity parent chain, returning `instance_key` itself
    /// for a top-level instance. Bounded against a pathological cycle.
    fn root_process_instance_key(&self, instance_key: Key) -> Key {
        let mut current = instance_key;
        // A legal parent chain is bounded by the same call-activity nesting
        // limit that guards deployment, so reuse it (plus one to reach the root
        // from the deepest legal instance) rather than a separate magic number.
        for _ in 0..=MAX_CALL_ACTIVITY_DEPTH {
            match self
                .state
                .instances
                .get(&current)
                .and_then(|i| i.parent_process_instance_key)
            {
                Some(parent) => current = parent,
                None => break,
            }
        }
        current
    }

    /// Locate the AgentInstance identified by `agent_instance_key`, returning a
    /// reference. Agent instances live inside their owning
    /// [`crate::state::ProcessInstance::agent_instances`], so this scans the
    /// live instances; the S3 processors call it once per command and the live
    /// set is small relative to a turn's cost.
    fn find_agent_instance(&self, agent_instance_key: Key) -> Option<&crate::agent::AgentInstance> {
        self.state
            .instances
            .values()
            .find_map(|inst| inst.agent_instances.get(&agent_instance_key))
    }

    /// The authoritative ownership tuple `(element_id, process_instance_key)` of
    /// the agent instance identified by `agent_instance_key`, or `None` when no
    /// such instance is live. The 8.10 `PATCH /agent-instances/{key}` REST
    /// request carries neither field, yet [`Command::UpdateAgentInstance`]
    /// asserts them for its ownership guard; the gateway resolves them here — on
    /// the engine thread, against the primary state — so the update is never
    /// built from an eventually-consistent read-model row.
    pub fn agent_instance_ownership(
        &self,
        agent_instance_key: Key,
    ) -> Option<(crate::model::ElementId, Key)> {
        self.find_agent_instance(agent_instance_key)
            .map(|inst| (inst.element_id.clone(), inst.process_instance_key))
    }

    /// Resolve an `element_instance_key` to its owning process-instance key and
    /// element id **only when it is active** (present in that instance's `active`
    /// map). Returns `None` for an unknown or already-completed element instance.
    /// The AgentInstance CREATE/UPDATE processors use this to enforce the
    /// "active element instance" precondition.
    fn resolve_active_element_instance(
        &self,
        element_instance_key: Key,
    ) -> Option<(Key, crate::model::ElementId)> {
        self.state.instances.iter().find_map(|(pi_key, inst)| {
            inst.active
                .get(&element_instance_key)
                .map(|element_id| (*pi_key, element_id.clone()))
        })
    }

    /// The agent instance (if any) that already owns `element_instance_key` —
    /// i.e. lists it in `element_instance_keys`. Used by the UPDATE processor to
    /// detect a *conflicting* instance before it links a re-entry key.
    fn owning_agent_instance(&self, element_instance_key: Key) -> Option<Key> {
        self.state.instances.values().find_map(|inst| {
            inst.agent_instances.values().find_map(|ai| {
                ai.element_instance_keys
                    .contains(&element_instance_key)
                    .then_some(ai.agent_instance_key)
            })
        })
    }

    /// Validate the job context an agent's
    /// `CreateAgentInstance` / `UpdateAgentInstance` carries — the nano-bpm analog
    /// of Camunda's `AgentHistoryBatchBehavior.validateJobContext`
    /// (#1099/#1106), reproducing its exact rule order and rejection kinds:
    ///
    /// - **No job supplied** (`job_key == 0`, nano's sentinel for the absent
    ///   `jobKey`): allowed only when there is **no** history (`!has_history`);
    ///   once a batch is attached the job becomes required
    ///   ([`EngineError::AgentInstanceJobRequiredForHistory`], 400 —
    ///   `jobKey == -1` short-circuit in Camunda).
    /// - The job must be **ACTIVATED** (unknown or non-activated →
    ///   [`EngineError::AgentInstanceJobNotActive`], 404).
    /// - **Conditional lease check** (Camunda's `hasLeaseToken()` gate): a job
    ///   carrying a lease token must match `job_lease`
    ///   ([`EngineError::AgentInstanceJobLeaseMismatch`], 404); a *lease-less*
    ///   job (`lease_token == None`) skips the comparison entirely.
    /// - The job must belong to `element_instance_key`
    ///   ([`EngineError::AgentInstanceJobElementMismatch`], 400).
    ///
    /// The lease "token" is the job's per-activation **staleness handle**
    /// ([`state::Job::lease_token`], distinct from `deadline`) — the value an
    /// activation response returns to the worker and which it echoes back as
    /// `job_lease`. Comparisons use opaque strings; only persisted legacy frames
    /// accept numeric tokens through the compatibility decoder.
    fn validate_agent_job_context(
        &self,
        element_instance_key: Key,
        job_key: Key,
        job_lease: &str,
        has_history: bool,
    ) -> Result<(), EngineError> {
        // No job supplied: only permitted when no history batch is attached.
        if job_key == 0 {
            if has_history {
                return Err(EngineError::AgentInstanceJobRequiredForHistory {
                    element_instance_key,
                });
            }
            return Ok(());
        }
        // The job must be a currently-activated job.
        let job = match self.state.jobs.get(&job_key) {
            Some(job) if job.state == state::JobState::Activated => job,
            _ => {
                return Err(EngineError::AgentInstanceJobNotActive {
                    element_instance_key,
                    job_key,
                });
            }
        };
        // Conditional lease comparison (Camunda `hasLeaseToken()`): a lease-less
        // activation carries no token and skips this check.
        if let Some(token) = job.lease_token.as_deref() {
            if token != job_lease {
                return Err(EngineError::AgentInstanceJobLeaseMismatch {
                    element_instance_key,
                    job_key,
                });
            }
        }
        // The job must belong to the requested element instance.
        if job.element_instance_key != element_instance_key {
            return Err(EngineError::AgentInstanceJobElementMismatch {
                job_key,
                job_element_instance_key: job.element_instance_key,
                element_instance_key,
            });
        }
        Ok(())
    }

    /// Process a `Command::CreateAgentInstance` (Camunda `AgentInstanceIntent.CREATE`,
    /// stable/8.10). The referenced `element_instance_key` must be an **active**
    /// job-worker element bearing a `zeebe:agentDefinition` marker, including
    /// job-backed ad-hoc containers. A service task without the marker is rejected as
    /// missing an agentDefinition, and any other element kind is not eligible.
    ///
    /// There is no activation-time record: the worker registers it here.
    /// Supplied `job_key`/`job_lease` must reference that element's **ACTIVATED** job
    /// with a matching lease token and `elementInstanceKey`
    /// ([`Self::validate_agent_job_context`]); otherwise the CREATE is rejected.
    /// History requires job attribution; history-free registration may omit it.
    /// A repeat CREATE conflicts. History-bearing registration derives definition
    /// and limits from CONFIGURATION, then records pending history.
    #[allow(clippy::too_many_arguments)]
    fn process_create_agent_instance(
        &mut self,
        log: &mut Vec<Event>,
        element_instance_key: Key,
        job_key: Key,
        job_lease: String,
        definition: crate::agent::AgentDefinition,
        limits: Option<crate::agent::AgentInstanceLimits>,
        mut history: Vec<crate::agent::AgentHistoryTurn>,
    ) -> Result<(), EngineError> {
        // 1. The element instance must be active.
        let (process_instance_key, element_id) = self
            .resolve_active_element_instance(element_instance_key)
            .ok_or(EngineError::AgentInstanceElementInstanceInactive {
                element_instance_key,
            })?;
        if let Some(agent_instance_key) = self.owning_agent_instance(element_instance_key) {
            return Err(EngineError::AgentInstanceAlreadyExists {
                element_instance_key,
                agent_instance_key,
            });
        }
        // 2. Its element must be agent-eligible and carry an agentDefinition.
        let agent_type =
            self.agent_type_for_element(process_instance_key, &element_id, element_instance_key)?;
        // 2a. Every supplied job is validated; history requires attribution.
        // History-free jobless registration follows Camunda's short-circuit.
        self.validate_agent_job_context(
            element_instance_key,
            job_key,
            &job_lease,
            !history.is_empty(),
        )?;
        Self::validate_agent_history(&history, true)?;
        for turn in &mut history {
            turn.job_key = job_key;
            turn.job_lease = job_lease.clone();
        }
        let agent_instance_key = self.mint_key();
        let root_process_instance_key = self.root_process_instance_key(process_instance_key);
        let (bpmn_process_id, process_definition_key, process_definition_version) = self
            .state
            .instances
            .get(&process_instance_key)
            .map(|inst| {
                let version = self
                    .state
                    .process_versions
                    .get(&inst.process_definition_key)
                    .map(|d| d.version)
                    .unwrap_or(0);
                (
                    inst.process_id.clone(),
                    inst.process_definition_key,
                    version,
                )
            })
            .unwrap_or_default();
        let mut agent_instance = crate::agent::AgentInstance {
            agent_instance_key,
            agent_definition_key: 0,
            element_instance_key,
            element_instance_keys: vec![element_instance_key],
            element_id: element_id.clone(),
            process_instance_key,
            root_process_instance_key,
            bpmn_process_id,
            process_definition_key,
            process_definition_version,
            process_definition_version_tag: None,
            tenant_id: crate::DEFAULT_TENANT.to_string(),
            agent_type,
            status: crate::agent::AgentInstanceStatus::Initializing,
            definition: if history.is_empty() {
                definition
            } else {
                Default::default()
            },
            limits: if history.is_empty() {
                limits.unwrap_or_default()
            } else {
                Default::default()
            },
            metrics: Default::default(),
            tools: Vec::new(),
            // Refresh attribution after reactivation rather than retaining a stale lease.
            job_key,
            job_lease,
            created_at: self.now,
            last_updated_at: self.now,
            completed_at: 0,
        };
        self.apply_history_changes(&mut agent_instance, &history, true);
        self.emit(
            log,
            Event::AgentInstanceCreated {
                instance_key: process_instance_key,
                agent_instance,
            },
        );
        // History stays pending until the attributed job resolves.
        if !history.is_empty() {
            let created = self.append_agent_history(agent_instance_key, history);
            log.extend(created);
        }
        Ok(())
    }

    /// Process a `Command::UpdateAgentInstance` (Camunda `AgentInstanceIntent.UPDATE`,
    /// stable/8.10). Validates that the instance exists, the asserted
    /// element/process instance matches, the referenced element instance is
    /// active and not owned by a *different* agent instance, and that the target
    /// `status` (if any) is an **active** state (`COMPLETED` is not settable via
    /// UPDATE). Enforces a single active writer, records history-derived metrics,
    /// and defers configuration changes until the attributed job commits.
    #[allow(clippy::too_many_arguments)]
    fn process_update_agent_instance(
        &mut self,
        log: &mut Vec<Event>,
        agent_instance_key: Key,
        element_instance_key: Key,
        element_id: crate::model::ElementId,
        process_instance_key: Key,
        job_key: Key,
        job_lease: String,
        status: Option<crate::agent::AgentInstanceStatus>,
        metrics: crate::agent::AgentInstanceMetricsDelta,
        tools: Option<Vec<crate::agent::AgentTool>>,
        mut history: Vec<crate::agent::AgentHistoryTurn>,
    ) -> Result<(), EngineError> {
        // 1. The instance must exist.
        let mut updated = self
            .find_agent_instance(agent_instance_key)
            .cloned()
            .ok_or(EngineError::AgentInstanceNotFound { agent_instance_key })?;
        // 2. The asserted ownership (element id + process instance) must match.
        if updated.element_id != element_id || updated.process_instance_key != process_instance_key
        {
            return Err(EngineError::AgentInstanceOwnershipMismatch { agent_instance_key });
        }
        // The actual element and process ownership precede association conflicts
        // and the job gate, including when the command proposes a re-entry key.
        let (active_pi, active_element_id) = self
            .resolve_active_element_instance(element_instance_key)
            .ok_or(EngineError::AgentInstanceElementInstanceInactive {
                element_instance_key,
            })?;
        if active_element_id != updated.element_id || active_pi != updated.process_instance_key {
            return Err(EngineError::AgentInstanceOwnershipMismatch { agent_instance_key });
        }
        // 4. No conflicting instance: the element instance may only be owned by
        //    this agent instance (or be a fresh re-entry key owned by none).
        if let Some(owner) = self.owning_agent_instance(element_instance_key) {
            if owner != agent_instance_key {
                return Err(EngineError::AgentInstanceConflict {
                    agent_instance_key,
                    element_instance_key,
                    conflicting_agent_instance_key: owner,
                });
            }
        }
        if updated.element_instance_key != element_instance_key
            && self.state.jobs.values().any(|job| {
                job.element_instance_key == updated.element_instance_key
                    && job.state == state::JobState::Activated
            })
        {
            return Err(EngineError::AgentInstanceActiveWriter {
                agent_instance_key,
                element_instance_key: updated.element_instance_key,
            });
        }
        self.validate_agent_job_context(
            element_instance_key,
            job_key,
            &job_lease,
            !history.is_empty(),
        )?;
        Self::validate_agent_history(&history, false)?;
        if !history.is_empty()
            && (metrics != crate::agent::AgentInstanceMetricsDelta::default() || tools.is_some())
        {
            return Err(EngineError::AgentHistoryInvalid {
                reason: "history-bearing UPDATE permits only status at request level".to_string(),
            });
        }
        if [
            metrics.input_tokens,
            metrics.output_tokens,
            metrics.model_calls,
            metrics.tool_calls,
        ]
        .iter()
        .any(|value| *value < -1)
        {
            return Err(EngineError::AgentHistoryInvalid {
                reason: "metric deltas must be -1 or nonnegative".to_string(),
            });
        }
        for turn in &mut history {
            turn.job_key = job_key;
            turn.job_lease = job_lease.clone();
        }
        // 5. The target status (if any) must be an active state.
        if let Some(target) = status {
            if !target.is_active()
                || (target == crate::agent::AgentInstanceStatus::Initializing
                    && updated.status != crate::agent::AgentInstanceStatus::Initializing)
            {
                return Err(EngineError::AgentInstanceStatusNotSettable {
                    agent_instance_key,
                    status: target,
                });
            }
        }
        // Legacy internal history-free deltas remain supported; REST derives metrics from history.
        let new_metrics = updated.metrics.with_delta(&metrics);
        // 7. Apply: status, metrics, tools, re-entry link, timestamp.
        if let Some(target) = status {
            updated.status = target;
        }
        updated.metrics = new_metrics;
        self.apply_history_changes(&mut updated, &history, false);
        if let Some(new_tools) = tools {
            updated.tools = new_tools;
        }
        // A supplied job was lease-validated (step 1a), so
        // record the freshly-proven attribution on the snapshot rather than
        // leaving a stale lease token — a later update keyed off the snapshot's
        // lease would otherwise be wrongly rejected by `validate_agent_job_context`.
        if job_key != 0 {
            updated.job_key = job_key;
            updated.job_lease = job_lease;
        }
        if !updated
            .element_instance_keys
            .contains(&element_instance_key)
        {
            updated.element_instance_keys.push(element_instance_key);
        }
        updated.element_instance_key = element_instance_key;
        updated.last_updated_at = self.now;
        let instance_key = updated.process_instance_key;
        self.emit(
            log,
            Event::AgentInstanceUpdated {
                instance_key,
                agent_instance: updated,
            },
        );
        // History stays pending until the attributed job resolves.
        if !history.is_empty() {
            let created = self.append_agent_history(agent_instance_key, history);
            log.extend(created);
        }
        Ok(())
    }

    /// Legacy embedded per-agent completion. The canonical lifecycle completes
    /// agents during process cleanup, while history resolves with its job.
    fn process_complete_agent_instance(
        &mut self,
        log: &mut Vec<Event>,
        agent_instance_key: Key,
    ) -> Result<(), EngineError> {
        let mut completed = self
            .find_agent_instance(agent_instance_key)
            .cloned()
            .ok_or(EngineError::AgentInstanceNotFound { agent_instance_key })?;
        if !completed.status.is_active() {
            return Err(EngineError::AgentInstanceAlreadyCompleted { agent_instance_key });
        }
        completed.status = crate::agent::AgentInstanceStatus::Completed;
        completed.completed_at = self.now;
        completed.last_updated_at = self.now;
        let instance_key = completed.process_instance_key;
        self.emit(
            log,
            Event::AgentInstanceCompleted {
                instance_key,
                agent_instance: completed,
            },
        );
        Ok(())
    }

    /// Append a batch of AgentHistory turns to `agent_instance_key`'s
    /// append-only turn log (Camunda 8.10 AgentHistory, slice S2). Each turn is
    /// materialised into its own [`crate::agent::AgentHistoryRecord`] with a
    /// freshly-minted, monotonic `agent_history_key` and `commit_status`
    /// PENDING, emitted as an [`Event::AgentHistoryCreated`]. The
    /// instance-derived context (element/process/root instance keys, bpmn
    /// process id, process-definition key, tenant) is copied from the owning
    /// AgentInstance; an unknown `agent_instance_key` yields no events.
    ///
    /// **Idempotent retry dedup.** A turn carrying a `historyItemId` that
    /// matches an already-recorded, non-discarded turn for this agent instance
    /// — whether recorded by a prior batch or earlier in *this* batch — is an
    /// idempotent retry: no new record is materialised and instead an
    /// [`Event::AgentHistoryDeduplicated`] naming the original turn's
    /// `agent_history_key` is emitted. Turns without a `historyItemId` (or with
    /// an empty one) cannot be correlated and always materialise a fresh record.
    /// This is what lets the API echo back `isDuplicate=true` with the original
    /// key while creating no duplicate AGENT_HISTORY record.
    ///
    /// This is the internal behavior the S3 CREATE/UPDATE processors invoke to
    /// apply a `history[]` batch. It emits (and applies) the events and returns
    /// them so the caller folds them into its command result.
    pub(crate) fn append_agent_history(
        &mut self,
        agent_instance_key: Key,
        turns: Vec<crate::agent::AgentHistoryTurn>,
    ) -> Vec<Event> {
        let mut log = Vec::new();
        // Copy the instance-derived context up front, releasing the immutable
        // borrow before we mint keys / emit (both need `&mut self`).
        let base = match self.find_agent_instance(agent_instance_key) {
            Some(ai) => AgentHistoryBase {
                instance_key: ai.process_instance_key,
                element_instance_key: ai.element_instance_key,
                process_instance_key: ai.process_instance_key,
                root_process_instance_key: ai.root_process_instance_key,
                bpmn_process_id: ai.bpmn_process_id.clone(),
                process_definition_key: ai.process_definition_key,
                tenant_id: ai.tenant_id.clone(),
            },
            None => return log,
        };
        for turn in turns {
            // Idempotent dedup by `historyItemId`. An absent/empty id cannot be
            // correlated to a prior turn, so it always materialises a fresh
            // record. A matching id (against prior state or an earlier turn in
            // this same batch) resolves to the original key and creates no new
            // AGENT_HISTORY record.
            if let Some(id) = turn.history_item_id.as_deref().filter(|id| !id.is_empty()) {
                if let Some(original_agent_history_key) =
                    self.duplicate_agent_history(agent_instance_key, &turn)
                {
                    self.emit(
                        &mut log,
                        Event::AgentHistoryDeduplicated {
                            instance_key: base.instance_key,
                            agent_instance_key,
                            history_item_id: id.to_string(),
                            original_agent_history_key,
                        },
                    );
                    continue;
                }
            }
            let agent_history_key = self.mint_key();
            let record = crate::agent::AgentHistoryRecord {
                changed_attributes: Self::configuration_attributes(&turn),
                agent_history_key,
                agent_instance_key,
                element_instance_key: base.element_instance_key,
                process_instance_key: base.process_instance_key,
                root_process_instance_key: base.root_process_instance_key,
                bpmn_process_id: base.bpmn_process_id.clone(),
                process_definition_key: base.process_definition_key,
                tenant_id: base.tenant_id.clone(),
                job_key: turn.job_key,
                job_lease: turn.job_lease,
                loop_iteration: turn.loop_iteration,
                role: turn.role,
                produced_at: turn.produced_at,
                content: turn.content,
                system_prompt: turn.system_prompt,
                tool_calls: turn.tool_calls,
                metrics: turn.metrics,
                history_item_id: turn.history_item_id,
                tools: turn.tools,
                model: turn.model,
                provider: turn.provider,
                limits: turn.limits,
                is_duplicate: turn.is_duplicate,
                commit_status: crate::agent::AgentHistoryCommitStatus::Pending,
            };
            self.emit(
                &mut log,
                Event::AgentHistoryCreated {
                    instance_key: base.instance_key,
                    record,
                },
            );
        }
        log
    }

    /// Commit `agent_instance_key`'s pending AgentHistory turns (PENDING ->
    /// COMMITTED). Emits a single [`Event::AgentHistoryCommitted`] naming the
    /// affected turns, or nothing when there are no pending turns / the instance
    /// is unknown. Test-only helper for exercising the low-level turn log.
    #[cfg(test)]
    pub(crate) fn commit_agent_history(&mut self, agent_instance_key: Key) -> Vec<Event> {
        self.transition_pending_agent_history(agent_instance_key, true)
    }

    /// Discard `agent_instance_key`'s pending AgentHistory turns (PENDING ->
    /// DISCARDED). Emits a single [`Event::AgentHistoryDiscarded`] naming the
    /// affected turns, or nothing when there are no pending turns / the instance
    /// is unknown. Process cleanup discards any unresolved attempts.
    pub(crate) fn discard_agent_history(&mut self, agent_instance_key: Key) -> Vec<Event> {
        self.transition_pending_agent_history(agent_instance_key, false)
    }

    /// Shared body of [`Self::commit_agent_history`] / [`Self::discard_agent_history`]:
    /// collect the currently-PENDING turn keys and emit the single lifecycle
    /// event (`commit` selects COMMITTED vs DISCARDED). Only PENDING turns are
    /// named, so committed/discarded turns stay immutable.
    fn transition_pending_agent_history(
        &mut self,
        agent_instance_key: Key,
        commit: bool,
    ) -> Vec<Event> {
        let mut log = Vec::new();
        let instance_key = match self.find_agent_instance(agent_instance_key) {
            Some(ai) => ai.process_instance_key,
            None => return log,
        };
        let pending: Vec<Key> = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|inst| inst.agent_history.get(&agent_instance_key))
            .map(|records| {
                records
                    .iter()
                    .filter(|r| r.commit_status == crate::agent::AgentHistoryCommitStatus::Pending)
                    .map(|r| r.agent_history_key)
                    .collect()
            })
            .unwrap_or_default();
        if pending.is_empty() {
            return log;
        }
        let event = if commit {
            Event::AgentHistoryCommitted {
                instance_key,
                agent_instance_key,
                agent_history_keys: pending,
            }
        } else {
            Event::AgentHistoryDiscarded {
                instance_key,
                agent_instance_key,
                agent_history_keys: pending,
            }
        };
        self.emit(&mut log, event);
        log
    }

    ///
    /// Only [`Event::ProcessDeployed`] events are applied: message-start
    /// subscriptions and timer-start arming are intentionally skipped so those
    /// start events remain owned by the single deployment partition (otherwise a
    /// timer-start would fire once per partition). Used by a multi-partition host
    /// to replicate partition 0's deployments to the others.
    pub fn install_deployment(&mut self, events: &[Event]) {
        for event in events {
            // Advance the local key generator past any installed key that belongs
            // to THIS partition, exactly as `replay_partition` does. This matters
            // when a partition installs a deployment it also minted (a Raft replica
            // actor for the deploy-owning partition): without it the replica's
            // instance-key counter lags the leader's by the number of keys the
            // deploy minted, so a replicated `CreateInstance` mints a divergent key
            // and the replica forks. Keys minted by OTHER partitions never advance
            // this counter (partition guard), so the common in-memory fan-out to
            // non-owning partitions is unchanged.
            let max_key = event.max_key();
            if state::partition_of(max_key) == self.partition_id {
                self.next_local = self.next_local.max(state::local_of(max_key));
            }
            if matches!(
                event,
                Event::ProcessDeployed { .. }
                    | Event::DecisionRequirementsDeployed { .. }
                    | Event::DecisionDeployed { .. }
                    | Event::FormDeployed { .. }
                    | Event::GenericResourceDeployed { .. }
            ) {
                state::apply(&mut self.state, event);
            }
        }
    }

    /// Like [`install_deployment`](Self::install_deployment) but applies each
    /// `ProcessDeployed` **only when it is newer** than the definition already
    /// registered for its process id (absent, or a strictly lower version).
    ///
    /// Used by segmented multi-partition recovery to replay a partition-agnostic
    /// deployment (a durable replicated copy, keyed to the deployment partition)
    /// into a partition that may already hold that definition from its own
    /// snapshot at an equal or newer version. Because compaction and per-partition
    /// snapshots advance on independent watermarks, an *older* surviving durable
    /// copy could otherwise regress the definition; the version guard makes the
    /// replay idempotent and monotonic. Key generation is advanced exactly as in
    /// [`install_deployment`](Self::install_deployment).
    pub fn install_deployment_if_newer(&mut self, events: &[Event]) {
        for event in events {
            let max_key = event.max_key();
            if state::partition_of(max_key) == self.partition_id {
                self.next_local = self.next_local.max(state::local_of(max_key));
            }
            if matches!(
                event,
                Event::ProcessDeployed { .. }
                    | Event::DecisionRequirementsDeployed { .. }
                    | Event::DecisionDeployed { .. }
                    | Event::FormDeployed { .. }
                    | Event::GenericResourceDeployed { .. }
            ) {
                // Always apply every deployment event: each applier retains every
                // version in its `*_versions` map and internally guards the
                // latest-by-id index with a `>=` check, so an out-of-order/older
                // durable copy never regresses the latest pointer. The previous
                // skip-older guard would drop historical versions that a
                // version-pinned binding, a running instance, or an
                // EvaluateDecision by an older key must still resolve.
                state::apply(&mut self.state, event);
            }
        }
    }

    /// Creates a fresh process instance and queues its start event for
    /// activation. Shared by `CreateInstance`, message-start correlation, and
    /// timer-start firing.
    #[allow(clippy::too_many_arguments)]
    fn start_instance(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        process_id: String,
        start_event: ElementId,
        variables: HashMap<String, Value>,
        tags: Vec<String>,
        business_id: Option<String>,
        process_definition_key: Key,
    ) -> Key {
        let instance_key = self.mint_key();
        // Pin to the given version; a `0` key (start-event / dispatch callers)
        // resolves to the current latest for this process id.
        let (process_definition_key, version) = self
            .state
            .process_versions
            .get(&process_definition_key)
            .or_else(|| self.state.processes.get(&process_id))
            .map(|d| (d.key, d.version))
            .unwrap_or((process_definition_key, 0));
        self.emit(
            log,
            Event::ProcessInstanceCreated {
                instance_key,
                process_id,
                variables,
                created_at: self.now,
                tags,
                business_id,
                process_definition_key,
                version,
                parent_process_instance_key: None,
                parent_element_instance_key: None,
            },
        );
        queue.push_back(Step::Activate {
            instance_key,
            element_id: start_event,
            scope: 0,
            via: None,
        });
        instance_key
    }

    /// Places a start-triggered (message-start / timer-start) instance. On a
    /// single-partition host it creates it locally — byte-identical to the
    /// historical path. In a multi-partition cluster it round-robins a target
    /// partition: a local target creates inline; a remote target emits a routable
    /// [`Event::StartInstanceDispatched`] carrying the full creation payload, and
    /// the host routes a `DispatchStartInstance` to that partition (which mints
    /// the instance in its own namespace). This spreads start-triggered load
    /// across the cluster instead of piling every such instance onto the deploy
    /// partition.
    #[allow(clippy::too_many_arguments)]
    fn start_or_dispatch_instance(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        process_id: String,
        start_event: ElementId,
        variables: HashMap<String, Value>,
        tags: Vec<String>,
        business_id: Option<String>,
    ) {
        if self.num_partitions <= 1 {
            self.start_instance(
                log,
                queue,
                process_id,
                start_event,
                variables,
                tags,
                business_id,
                0,
            );
            return;
        }
        let target = self.start_dispatch_rr % self.num_partitions;
        self.start_dispatch_rr = self.start_dispatch_rr.wrapping_add(1);
        if target == self.partition_id {
            self.start_instance(
                log,
                queue,
                process_id,
                start_event,
                variables,
                tags,
                business_id,
                0,
            );
        } else {
            self.emit(
                log,
                Event::StartInstanceDispatched {
                    process_id,
                    start_element_id: start_event,
                    variables,
                    tags,
                    business_id,
                    target_partition: target,
                },
            );
        }
    }

    /// Validates and registers a batch of process definitions as one deployment.
    ///
    /// All processes are validated first, so the deployment is atomic: if any is
    /// invalid, none are emitted. Each *new or changed* process is assigned a
    /// unique process-definition key and a per-id version (latest known + 1).
    ///
    /// Deployment is idempotent: a process that is byte-for-byte identical to the
    /// current latest version of the same id (`existing.definition == process`,
    /// which includes the verbatim BPMN [`ProcessDefinition::xml`]) reuses that
    /// version's identity and emits **nothing** — no new key, no version bump, no
    /// journaled event. This mirrors Zeebe, where redeploying an unchanged
    /// resource does not create a new version, and keeps repeated idempotent
    /// deploys (e.g. on every app startup) from growing the journal. Keys are
    /// minted only for emitted events so replay (which derives the key counter
    /// from the max key seen in the log) stays in lock-step with the live engine.
    fn deploy(
        &mut self,
        log: &mut Vec<Event>,
        mut processes: Vec<ProcessDefinition>,
    ) -> Result<(), EngineError> {
        for process in &mut processes {
            process.normalize_legacy_agent_tasks();
        }
        for process in &processes {
            if !process.elements.contains_key(&process.start_event) {
                return Err(EngineError::NoStartEvent {
                    process_id: process.id.clone(),
                });
            }
        }

        // Emit DeploymentCreated FIRST, unconditionally — this is the C8-spec
        // handle the client sees in the response envelope (LongKey pattern,
        // never empty) and mirrors Zeebe's DeploymentIntent.CREATED. The
        // shared deployment_key is minted here and stamped onto every
        // ProcessDeployed that follows in the same call (issue #47).
        let deployment_key = self.mint_key();
        self.emit(log, Event::DeploymentCreated { deployment_key });

        for process in processes {
            if self
                .state
                .processes
                .get(&process.id)
                .is_some_and(|existing| existing.definition == process)
            {
                // Idempotent redeploy of the latest version: reuse its identity
                // and skip it entirely (no event, no new subscription/timer).
                continue;
            }
            let version = self.next_version(&process.id);
            let process_definition_key = self.mint_key();
            let process_id = process.id.clone();
            // Zeebe permits a process to declare several *typed*
            // (message/timer) start events alongside an optional none start,
            // and EVERY one of them arms its own trigger at deploy — a message
            // start opens a subscription, a timer start arms a process-level
            // timer — each firing an independent instance at its own start
            // element (issue #855). `process.start_event` designates only the
            // single CreateInstance entry point; it does NOT limit which starts
            // are wired. So scan all process-level (`parent.is_none()`) typed
            // starts, deterministically ordered by element id so key minting
            // and replay stay in lock-step.
            let mut typed_starts: Vec<(ElementId, ElementKind, Option<crate::model::TimerDef>)> =
                process
                    .elements
                    .values()
                    .filter(|e| e.parent.is_none())
                    .filter(|e| {
                        matches!(
                            e.kind,
                            ElementKind::MessageStartEvent { .. }
                                | ElementKind::TimerStartEvent { .. }
                        )
                    })
                    .map(|e| (e.id.clone(), e.kind.clone(), e.timer.clone()))
                    .collect();
            typed_starts.sort_by(|a, b| a.0.cmp(&b.0));
            self.emit(
                log,
                Event::ProcessDeployed {
                    deployment_key,
                    process_definition_key,
                    version,
                    process,
                },
            );
            for (start_element_id, start_kind, start_timer_def) in typed_starts {
                match start_kind {
                    ElementKind::MessageStartEvent { message_name } => {
                        // Per Zeebe, a start-event name expression is evaluated
                        // at deploy time against an empty context; a static name
                        // passes through unchanged.
                        let message_name = self.resolve_event_name(&HashMap::new(), &message_name);
                        self.emit(
                            log,
                            Event::MessageStartSubscriptionCreated {
                                process_definition_key,
                                process_id: process_id.clone(),
                                message_name,
                                start_element_id,
                            },
                        );
                    }
                    ElementKind::TimerStartEvent {
                        interval_millis,
                        repeating,
                    } => {
                        let timer_key = self.mint_key();
                        // Evaluate a FEEL start-timer expression against an empty
                        // context at deploy; a static literal falls back to the
                        // parsed interval. For a cycle the resolved interval is
                        // persisted so re-arming recurs on the same delay.
                        let (due_at, interval_millis) = self.resolve_timer(
                            &HashMap::new(),
                            start_timer_def.as_ref(),
                            self.now,
                            interval_millis,
                        );
                        self.emit(
                            log,
                            Event::ProcessStartTimerArmed {
                                timer_key,
                                process_definition_key,
                                process_id: process_id.clone(),
                                start_element_id,
                                due_at,
                                interval_millis,
                                repeating,
                            },
                        );
                    }
                    _ => {}
                }
            }
        }
        Ok(())
    }

    /// The version that the next deployment of `process_id` will receive.
    fn next_version(&self, process_id: &str) -> i32 {
        self.state
            .processes
            .get(process_id)
            .map(|p| p.version + 1)
            .unwrap_or(1)
    }

    /// Registers one or more decision requirements graphs as a deployment,
    /// mirroring [`Engine::deploy`]: a shared deployment key, one
    /// [`Event::DecisionRequirementsDeployed`] per DRG, and one
    /// [`Event::DecisionDeployed`] per decision it contains (indexed by id).
    /// An idempotent redeploy of the identical latest DRG is skipped.
    fn deploy_decisions(
        &mut self,
        log: &mut Vec<Event>,
        graphs: Vec<crate::dmn::DecisionRequirementsGraph>,
    ) -> Result<(), EngineError> {
        let deployment_key = self.mint_key();
        self.emit(log, Event::DeploymentCreated { deployment_key });

        for drg in graphs {
            if self
                .state
                .decision_requirements
                .get(&drg.id)
                .is_some_and(|existing| existing.drg == drg)
            {
                // Idempotent redeploy of the identical latest DRG: skip.
                continue;
            }
            let version = self
                .state
                .decision_requirements
                .get(&drg.id)
                .map(|d| d.version + 1)
                .unwrap_or(1);
            let decision_requirements_key = self.mint_key();

            // Emit the DRG registration first so the DecisionDeployed applier can
            // resolve the graph it belongs to.
            let decisions = drg.decisions.clone();
            self.emit(
                log,
                Event::DecisionRequirementsDeployed {
                    deployment_key,
                    decision_requirements_key,
                    version,
                    drg,
                },
            );

            for decision in &decisions {
                let decision_version = self
                    .state
                    .decisions
                    .get(&decision.id)
                    .map(|d| d.version + 1)
                    .unwrap_or(1);
                let decision_key = self.mint_key();
                self.emit(
                    log,
                    Event::DecisionDeployed {
                        deployment_key,
                        decision_requirements_key,
                        decision_key,
                        decision_id: decision.id.clone(),
                        decision_name: decision.name.clone(),
                        version: decision_version,
                    },
                );
            }
        }
        Ok(())
    }

    /// Registers one or more forms as a deployment, mirroring
    /// [`Engine::deploy_decisions`]: a shared deployment key and one
    /// [`Event::FormDeployed`] per form (versioned per form id). An idempotent
    /// redeploy of the identical latest form is skipped. The engine does not
    /// execute forms; it stores them so `GetFormByKey` can serve the schema.
    fn deploy_forms(
        &mut self,
        log: &mut Vec<Event>,
        forms: Vec<crate::command::FormResource>,
    ) -> Result<(), EngineError> {
        let deployment_key = self.mint_key();
        self.emit(log, Event::DeploymentCreated { deployment_key });

        for form in forms {
            if self
                .state
                .forms
                .get(&form.id)
                .is_some_and(|existing| existing.schema == form.schema)
            {
                // Idempotent redeploy of the identical latest form: skip.
                continue;
            }
            let version = self
                .state
                .forms
                .get(&form.id)
                .map(|f| f.version + 1)
                .unwrap_or(1);
            let form_key = self.mint_key();
            self.emit(
                log,
                Event::FormDeployed {
                    deployment_key,
                    form_key,
                    version,
                    form_id: form.id,
                    resource_name: form.resource_name,
                    schema: form.schema,
                },
            );
        }
        Ok(())
    }

    /// Registers one or more generic resources as a deployment, mirroring
    /// [`Engine::deploy_forms`]: a shared deployment key and one
    /// [`Event::GenericResourceDeployed`] per resource (versioned per
    /// `resource_id`). An idempotent redeploy of the identical latest resource
    /// (same `resource_name` and content — Zeebe's name+checksum duplicate rule)
    /// is skipped. The engine does not execute generic resources; it stores them
    /// so `GetResourceByKey` / `searchResources` can serve the content.
    fn deploy_generic_resources(
        &mut self,
        log: &mut Vec<Event>,
        resources: Vec<crate::command::GenericResource>,
    ) -> Result<(), EngineError> {
        let deployment_key = self.mint_key();
        self.emit(log, Event::DeploymentCreated { deployment_key });

        for resource in resources {
            if self
                .state
                .resources
                .get(&resource.resource_id)
                .is_some_and(|existing| {
                    existing.resource_name == resource.resource_name
                        && existing.content == resource.content
                })
            {
                // Idempotent redeploy of the identical latest resource: skip.
                continue;
            }
            let version = self
                .state
                .resources
                .get(&resource.resource_id)
                .map(|r| r.version + 1)
                .unwrap_or(1);
            let resource_key = self.mint_key();
            self.emit(
                log,
                Event::GenericResourceDeployed {
                    deployment_key,
                    resource_key,
                    version,
                    resource_id: resource.resource_id,
                    resource_name: resource.resource_name,
                    content: resource.content,
                },
            );
        }
        Ok(())
    }

    /// Records one decision evaluation as an [`Event::DecisionEvaluated`]: the
    /// one place every evaluation — a businessRuleTask's (successful or failed)
    /// and the standalone EvaluateDecision command's — becomes an event, so they
    /// cannot drift (issue #1292; Zeebe `DecisionBehavior.createDecisionEvaluationEvent`).
    /// Mints the evaluation's own key from the partition key generator and
    /// stamps every evaluated decision with the exact definition (key, version)
    /// evaluated, resolved within `deployed`'s DRG version. `instance_key` /
    /// `element_instance_key` are `0` and `element_id` empty for a standalone
    /// evaluation.
    fn decision_evaluated_event(
        &mut self,
        deployed: &state::DeployedDecision,
        result: crate::dmn::DecisionEvaluationResult,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
    ) -> Event {
        let decision_evaluation_key = self.mint_key();
        let mut evaluated_decisions = result.evaluated_decisions;
        for evaluated in &mut evaluated_decisions {
            if let Some(definition) = self
                .state
                .decision_in_drg(deployed.decision_requirements_key, &evaluated.decision_id)
            {
                evaluated.decision_key = definition.key;
                evaluated.decision_version = definition.version;
            }
        }
        Event::DecisionEvaluated {
            instance_key,
            element_instance_key,
            element_id,
            decision_key: deployed.key,
            decision_id: deployed.decision_id.clone(),
            decision_output: result.decision_output,
            evaluated_decisions,
            evaluated_at: self.now,
            decision_evaluation_key,
            failure: result.failure,
            decision_requirements_key: deployed.decision_requirements_key,
            decision_requirements_id: deployed.drg.id.clone(),
        }
    }

    /// Applies a command using the engine's current clock reading (see
    /// [`Engine::apply_command_at`]). Tests and hosts that do not need accurate
    /// timestamps can use this; the default clock is `0`.
    pub fn apply_command(&mut self, command: Command) -> Result<Vec<Event>, EngineError> {
        self.apply_command_at(command, self.now)
    }

    /// Applies a command, returning the ordered list of events it produced,
    /// stamping any timestamped events (e.g. raised incidents) with `now`.
    ///
    /// This is the engine's single writer: it runs to quiescence before
    /// returning, so on success the returned events are the complete record of
    /// everything that happened. `now` is the host's clock reading for this
    /// command; the engine never reads a wall clock itself.
    ///
    /// Structure: [`plan_command_at`](Self::plan_command_at) translates the
    /// command into an initial `(log, queue)`, the RTC [`run`](Self::run) drives
    /// the queue to quiescence, and [`finish_command`](Self::finish_command)
    /// performs the post-drain tail. The debug entrypoints reuse the same
    /// planner + tail with a *stepping* driver in place of `run`.
    pub fn apply_command_at(
        &mut self,
        command: Command,
        now: u64,
    ) -> Result<Vec<Event>, EngineError> {
        let (mut log, queue) = self.plan_command_at(command, now)?;
        self.run(&mut log, queue);
        self.cascade_cancel_children(&mut log);
        self.finish_command(&log);
        Ok(log)
    }

    /// Post-drain tail shared by [`apply_command_at`](Self::apply_command_at) and
    /// the debug entrypoints. Instance completion now happens at the fixpoint
    /// drain point inside [`run_with`](Self::run_with) (so a debug
    /// `ProcessCompleted` breakpoint can observe it through the driver); the only
    /// work left here is the follower-only tombstone reap, which is orthogonal to
    /// completion. Runs after the queue has drained to quiescence.
    fn finish_command(&mut self, log: &[Event]) {
        // Follower-only safety net: if a retirement digest raced ahead of an
        // instance's create on this replica (leader-durable async-learner lag), the
        // key was tombstoned; now that the create has materialized the instance,
        // reap it immediately so it cannot linger as a never-retired `Active` shell.
        // Near-free when nothing is pending (a leader never tombstones).
        if !self.retired_tombstones.is_empty() {
            let created: Vec<Key> = log
                .iter()
                .filter_map(|e| match e {
                    Event::ProcessInstanceCreated { instance_key, .. } => Some(*instance_key),
                    _ => None,
                })
                .collect();
            if !created.is_empty() {
                self.reap_tombstoned(&created);
            }
        }
    }

    /// Translates `command` into the initial `(log, queue)` the fixpoint loop
    /// drives, without running it. Splitting planning from driving is what makes
    /// stepping possible: the production path calls `run` on the queue, the
    /// debugger drives it one step at a time. `now` stamps timestamped events.
    fn plan_command_at(
        &mut self,
        command: Command,
        now: u64,
    ) -> Result<(Vec<Event>, VecDeque<Step>), EngineError> {
        self.now = now;
        // Clear the per-command scratch of sub-process scopes torn down mid-drain
        // (see [`torn_down_scopes`]): each command drains independently, so a
        // scope invalidated by a previous command's terminate must not leak into
        // this one's dead-scope guard.
        self.torn_down_scopes.clear();
        let mut log: Vec<Event> = Vec::new();
        let mut queue: VecDeque<Step> = VecDeque::new();

        match command {
            Command::DeployProcess(process) => {
                self.deploy(&mut log, vec![process])?;
            }

            Command::DeployResources(processes) => {
                self.deploy(&mut log, processes)?;
            }

            Command::DeployDecisionRequirements(graphs) => {
                self.deploy_decisions(&mut log, graphs)?;
            }

            Command::DeployForms(forms) => {
                self.deploy_forms(&mut log, forms)?;
            }

            Command::DeployGenericResources(resources) => {
                self.deploy_generic_resources(&mut log, resources)?;
            }

            Command::DeleteDecisionInstance {
                instance_key,
                decision_evaluation_key,
            } => {
                // Audit-only retraction: emit the event so the read-model
                // projection deletes the matching decision-instance rows. No core
                // engine state is touched (decision instances are not engine state).
                self.emit(
                    &mut log,
                    Event::DecisionInstanceDeleted {
                        instance_key,
                        decision_evaluation_key,
                    },
                );
            }

            Command::EvaluateDecision {
                decision,
                variables,
            } => {
                // Zeebe `DecisionEvaluationEvaluateProcessor`: resolve, evaluate,
                // and record the evaluation (successful or failed) under a freshly
                // minted evaluation key, with no process instance.
                let deployed = match &decision {
                    crate::command::DecisionReference::Id(id) => self.state.decisions.get(id),
                    crate::command::DecisionReference::Key(key) => self.state.decision_by_key(*key),
                }
                .cloned()
                .ok_or_else(|| EngineError::DecisionNotFound {
                    reference: match &decision {
                        crate::command::DecisionReference::Id(id) => format!("id '{id}'"),
                        crate::command::DecisionReference::Key(key) => format!("key {key}"),
                    },
                })?;
                let result = crate::dmn::evaluate(&deployed.drg, &deployed.decision_id, &variables);
                let event =
                    self.decision_evaluated_event(&deployed, result, 0, 0, ElementId::default());
                self.emit(&mut log, event);
            }

            Command::CreateInstance {
                process_id,
                variables,
                tags,
                business_id,
                process_definition_key,
                version,
            } => {
                check_business_id(business_id.as_deref())?;
                // Resolve the requested version to a concrete deployed
                // definition (Zeebe parity):
                //   * an explicit definition key selects that exact version
                //     (creation-by-key — the key already identifies the version);
                //   * else an explicit positive version number selects that
                //     version of `process_id` (creation-by-id + version);
                //   * else the latest version of `process_id`.
                let process = match (
                    process_definition_key.filter(|k| *k != 0),
                    version.filter(|v| *v > 0),
                ) {
                    (Some(key), _) => self.state.process_by_key(key).ok_or_else(|| {
                        EngineError::ProcessNotFound {
                            process_id: process_id.clone(),
                        }
                    })?,
                    (None, Some(v)) => {
                        self.state.process_version(&process_id, v).ok_or_else(|| {
                            EngineError::ProcessNotFound {
                                process_id: process_id.clone(),
                            }
                        })?
                    }
                    (None, None) => self.state.processes.get(&process_id).ok_or_else(|| {
                        EngineError::ProcessNotFound {
                            process_id: process_id.clone(),
                        }
                    })?,
                };
                let resolved_key = process.key;
                // A by-key request may carry an empty `process_id`; use the
                // resolved definition's id so the event and indices are coherent.
                let process_id = process.definition.id.clone();
                let start_event = process.definition.start_event.clone();
                self.start_instance(
                    &mut log,
                    &mut queue,
                    process_id,
                    start_event,
                    variables,
                    tags,
                    business_id,
                    resolved_key,
                );
            }

            Command::CompleteJob {
                job_key,
                lease_token,
                variables,
                adhoc_result,
                task_listener_result,
                business_id,
            } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                if matches!(
                    job.state,
                    state::JobState::Completed
                        | state::JobState::Failed
                        | state::JobState::Errored
                        | state::JobState::Canceled
                ) {
                    return Err(EngineError::JobNotActive { job_key });
                }
                Self::validate_job_lease(job, lease_token.as_deref(), true)?;
                // A job must have been activated at least once. For leased jobs,
                // the fence above also requires the current activation token. Under lenient
                // completion (leader-local locks; see `lenient_completion`) the
                // activation may not have been replicated to this engine, so the
                // latch is not required.
                if !self.lenient_completion && !job.activated {
                    return Err(EngineError::JobNotActivated { job_key });
                }
                let instance_key = job.instance_key;
                let element_instance_key = job.element_instance_key;
                let element_id = job.element_id.clone();
                let created_at = job.created_at;
                let job_type = job.job_type.clone();
                // The activating worker, stamped from the live job at the
                // completion site so a *successful* completion carries it forward
                // for read-model/export attribution (symmetric with the
                // `JobFailed` / `JobErrorThrown` paths). `Job.worker` is cleared
                // on completion, so it must be captured here before the job is
                // removed from state. `attribution_worker()` normalizes a
                // snapshot-restored empty worker (`Some("")`) to `None`, so a
                // pre-normalization snapshot can never leak an empty attribution.
                let worker = job.attribution_worker();
                let job_kind = job.kind;

                // A task-listener job (ADR 0037 §6) gates a user task's deferred
                // transition. Validate the completion up-front so a bad one is
                // rejected wholesale: task-listener jobs may not carry variables,
                // deny is only honoured on assigning/updating/completing, and a
                // denial cannot also carry corrections (Zeebe parity).
                if let state::JobKind::TaskListener {
                    event_type,
                    user_task_key,
                    ..
                } = job_kind
                {
                    let result = task_listener_result.clone().unwrap_or_default();
                    if !variables.is_empty() {
                        return Err(EngineError::TaskListenerJobWithVariables { job_key });
                    }
                    if result.denied && !result.corrections.is_empty() {
                        return Err(EngineError::TaskListenerDenyWithCorrections { job_key });
                    }
                    if result.denied && !Self::task_event_supports_deny(event_type) {
                        return Err(EngineError::TaskListenerDenyNotSupported { job_key });
                    }
                    // A creating listener cannot correct the assignee when the
                    // task already declares an initial assignee (Zeebe parity).
                    if matches!(event_type, crate::model::TaskListenerEventType::Creating)
                        && result.corrections.assignee.is_some()
                        && self
                            .state
                            .user_tasks
                            .get(&user_task_key)
                            .map(|t| {
                                // An initial assignee may already be applied to the
                                // record, or (when `assigning` listeners exist) be
                                // stripped off and carried on the pending creating
                                // transition for later routing — either forbids a
                                // creating assignee correction (Zeebe parity).
                                t.assignee.is_some()
                                    || t.pending
                                        .as_ref()
                                        .map(|p| p.assignee.is_some())
                                        .unwrap_or(false)
                            })
                            .unwrap_or(false)
                    {
                        return Err(EngineError::TaskListenerAssigneeCorrectionOnCreating {
                            user_task_key,
                        });
                    }
                }

                // Ad-hoc agent job completion (ADR 0023 seam 2 / #614 gap 4):
                // validate the activate-element instructions up-front so a bad
                // turn is rejected wholesale (Zeebe parity) before any
                // `JobCompleted` or activation side effect applies. The check
                // only fires for a container's agent job (its element id is in
                // the ad-hoc catalog); the tools' own jobs are not, so they fall
                // through unchanged.
                if let Some(result) = adhoc_result.as_ref() {
                    if let Some(def) = self.adhoc_def_of(instance_key, &element_id) {
                        // Every activated id must name one of the container's
                        // tools (Zeebe NOT_FOUND, checked first — see
                        // JobCompleteProcessor.checkAdHocSubprocessActivationTargetsAreValid);
                        // otherwise the loop would mint a phantom child that
                        // immediately completes.
                        Self::validate_adhoc_activation_targets(
                            &def,
                            instance_key,
                            &result.activate_elements,
                        )?;
                        // Asserting the completion condition is fulfilled while
                        // also requesting activations is contradictory (Zeebe
                        // INVALID_ARGUMENT — checkAdHocSubProcessCompletionCondition
                        // NotFulfilledForElementActivation).
                        if result.completion_condition_fulfilled
                            && !result.activate_elements.is_empty()
                        {
                            return Err(EngineError::AdHocActivateWithCompletion { job_key });
                        }
                    }
                }

                // Camunda 8.10 business-id assignment on completion, validated
                // last (JobCompleteProcessor order) so every rejection leaves
                // the job open; `Some` only when a new id is actually assigned.
                let assign_business_id =
                    self.validate_business_id_assignment(instance_key, business_id)?;
                if let Some(business_id) = assign_business_id {
                    self.emit(
                        &mut log,
                        Event::ProcessInstanceBusinessIdAssigned {
                            instance_key,
                            business_id,
                        },
                    );
                }

                self.emit(
                    &mut log,
                    Event::JobCompleted {
                        job_key,
                        instance_key,
                        created_at,
                        job_type,
                        worker,
                    },
                );
                if !variables.is_empty() {
                    // Job result variables propagate from the task's enclosing
                    // (flow) scope upward — each name updates the nearest ancestor
                    // scope that defines it, defaulting to root. For a root-only
                    // instance this collapses to the flat `VariablesUpdated`,
                    // byte-identical to the pre-scoping engine.
                    let flow_scope = self.scope_of(instance_key, element_instance_key);
                    for event in self.propagated_updates(instance_key, flow_scope, variables, false)
                    {
                        self.emit(&mut log, event);
                    }
                }
                // A task-listener job: its completion advances (or denies) the
                // user task's deferred transition rather than resuming a token.
                if let state::JobKind::TaskListener {
                    event_type,
                    index,
                    user_task_key,
                } = job_kind
                {
                    let result = task_listener_result.unwrap_or_default();
                    if result.denied {
                        // The transition is rejected: clear the pending state so
                        // the task returns to its prior available state. The
                        // reason is journaled on the resolution event.
                        self.emit(
                            &mut log,
                            Event::UserTaskTransitionResolved {
                                user_task_key,
                                instance_key,
                                denied: Some(result.denied_reason.unwrap_or_default()),
                            },
                        );
                    } else {
                        if !result.corrections.is_empty() {
                            self.emit(
                                &mut log,
                                Event::UserTaskCorrectionsApplied {
                                    user_task_key,
                                    instance_key,
                                    corrections: result.corrections,
                                },
                            );
                        }
                        queue.push_back(Step::AdvanceTaskListener {
                            user_task_key,
                            event_type,
                            index,
                        });
                    }
                }
                // An execution-listener job (ADR 0037): its completion advances the
                // element's listener chain rather than resuming the token. The
                // JobCompleted + variable merge above still apply (a listener may
                // contribute variables that later listeners and the element see).
                else if let state::JobKind::ExecutionListener {
                    event_type,
                    index,
                    scope,
                } = job_kind
                {
                    queue.push_back(Step::AdvanceListener {
                        instance_key,
                        element_instance_key,
                        element_id,
                        event_type,
                        index,
                        scope,
                    });
                } else if self.adhoc_def_of(instance_key, &element_id).is_some() {
                    // An ad-hoc container's agent job: instead of resuming the
                    // container token, drive the activate-element loop (ADR 0023
                    // seam 2). The container element id is in the definition's
                    // ad-hoc catalog; ordinary jobs (including the tools' own jobs)
                    // are not, so they fall through to the normal token resume.
                    let container_key = element_instance_key;
                    let result = adhoc_result.unwrap_or_default();
                    if result.completion_condition_fulfilled {
                        // The agent asserts the container's completion condition is
                        // met (Camunda `isCompletionConditionFulfilled`): complete
                        // now, cancelling any tools still running so none is orphaned
                        // (ADR 0023 seam 4). This is honoured independently of the
                        // engine-side `<completionCondition>` FEEL (evaluated per
                        // tool completion), so an agent can end the loop even when no
                        // static condition is declared.
                        queue.push_back(Step::CompleteAdHoc {
                            instance_key,
                            container_key,
                            cancel: true,
                        });
                    } else {
                        // Ordinary turn (possibly `cancelRemainingInstances`):
                        // shared with the external activate-activities command
                        // (#614 gap 3) so both seams activate identically.
                        self.enqueue_adhoc_turn(
                            &mut queue,
                            instance_key,
                            container_key,
                            result.activate_elements,
                            result.cancel_remaining_instances,
                        );
                    }
                } else {
                    // The parked service-task token resumes from ACTIVATED.
                    queue.push_back(Step::Complete {
                        instance_key,
                        element_instance_key,
                        element_id,
                    });
                }
            }

            Command::AssignUserTask {
                user_task_key,
                assignee,
                allow_override,
            } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created || task.pending.is_some() {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                // Mirror Camunda: when override is disallowed and the task is
                // already assigned, reject so it must be unassigned first.
                if !allow_override && task.assignee.is_some() {
                    return Err(EngineError::UserTaskAlreadyAssigned { user_task_key });
                }
                let instance_key = task.instance_key;
                let element_instance_key = task.element_instance_key;
                let element_id = task.element_id.clone();
                let listeners = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Assigning,
                );
                if let Some(first) = listeners.first().cloned() {
                    // Defer the assignment behind the assigning listener chain.
                    let pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Assigning,
                        assignee: Some(assignee),
                        update: None,
                        variables: std::collections::HashMap::new(),
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    for event in self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Assigning,
                        pending,
                        &first,
                    ) {
                        self.emit(&mut log, event);
                    }
                } else {
                    self.emit(
                        &mut log,
                        Event::UserTaskAssigned {
                            user_task_key,
                            instance_key,
                            assignee: Some(assignee),
                        },
                    );
                }
            }

            Command::UnassignUserTask { user_task_key } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created || task.pending.is_some() {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                let element_instance_key = task.element_instance_key;
                let element_id = task.element_id.clone();
                let listeners = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Assigning,
                );
                if let Some(first) = listeners.first().cloned() {
                    let pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Assigning,
                        assignee: None,
                        update: None,
                        variables: std::collections::HashMap::new(),
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    for event in self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Assigning,
                        pending,
                        &first,
                    ) {
                        self.emit(&mut log, event);
                    }
                } else {
                    self.emit(
                        &mut log,
                        Event::UserTaskAssigned {
                            user_task_key,
                            instance_key,
                            assignee: None,
                        },
                    );
                }
            }

            Command::UpdateUserTask {
                user_task_key,
                changeset,
            } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created || task.pending.is_some() {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                let element_instance_key = task.element_instance_key;
                let element_id = task.element_id.clone();
                // Normalise empty-string dates to "reset" (None), matching the
                // REST contract ("Reset by providing an empty String").
                let normalize =
                    |d: Option<String>| -> Option<String> { d.filter(|s| !s.is_empty()) };
                let candidate_groups = changeset.candidate_groups;
                let candidate_users = changeset.candidate_users;
                let due_date = changeset.due_date.map(normalize);
                let follow_up_date = changeset.follow_up_date.map(normalize);
                let priority = changeset.priority;
                let listeners = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Updating,
                );
                if let Some(first) = listeners.first().cloned() {
                    let pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Updating,
                        assignee: None,
                        update: Some(state::PendingUserTaskUpdate {
                            candidate_groups,
                            candidate_users,
                            due_date,
                            follow_up_date,
                            priority,
                        }),
                        variables: std::collections::HashMap::new(),
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    for event in self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Updating,
                        pending,
                        &first,
                    ) {
                        self.emit(&mut log, event);
                    }
                } else {
                    self.emit(
                        &mut log,
                        Event::UserTaskUpdated {
                            user_task_key,
                            instance_key,
                            candidate_groups,
                            candidate_users,
                            due_date,
                            follow_up_date,
                            priority,
                        },
                    );
                }
            }

            Command::CompleteUserTask {
                user_task_key,
                variables,
            } => {
                let task = self
                    .state
                    .user_tasks
                    .get(&user_task_key)
                    .ok_or(EngineError::UserTaskNotFound { user_task_key })?;
                if task.state != state::UserTaskState::Created || task.pending.is_some() {
                    return Err(EngineError::UserTaskNotActive { user_task_key });
                }
                let instance_key = task.instance_key;
                let element_instance_key = task.element_instance_key;
                let element_id = task.element_id.clone();

                let listeners = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Completing,
                );
                if let Some(first) = listeners.first().cloned() {
                    // Defer completion behind the completing listener chain; the
                    // completion variables ride on the pending transition and are
                    // applied when the chain drains.
                    let pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Completing,
                        assignee: None,
                        update: None,
                        variables,
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    for event in self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Completing,
                        pending,
                        &first,
                    ) {
                        self.emit(&mut log, event);
                    }
                } else {
                    self.emit(
                        &mut log,
                        Event::UserTaskCompleted {
                            user_task_key,
                            instance_key,
                        },
                    );
                    if !variables.is_empty() {
                        // User-task completion variables propagate from the task's
                        // enclosing (flow) scope upward, defaulting to root (flat
                        // `VariablesUpdated` for a root-only instance).
                        let flow_scope = self.scope_of(instance_key, element_instance_key);
                        for event in
                            self.propagated_updates(instance_key, flow_scope, variables, false)
                        {
                            self.emit(&mut log, event);
                        }
                    }
                    // The parked user-task token resumes from ACTIVATED.
                    queue.push_back(Step::Complete {
                        instance_key,
                        element_instance_key,
                        element_id,
                    });
                }
            }

            Command::ActivateJobs {
                job_type,
                worker,
                max_jobs,
                timeout,
                now,
                fetch_variables,
                with_lease,
            } => {
                let keys = self.select_activatable_job_keys(&job_type, max_jobs, now, with_lease);
                self.apply_activation_plan(
                    &mut log,
                    ActivationPlan {
                        job_keys: keys,
                        worker,
                        timeout,
                        now,
                        options: crate::JobActivationOptions {
                            fetch_variables,
                            with_lease,
                        },
                        durable: false,
                    },
                );
            }
            Command::ActivateJobsByKey {
                job_keys,
                worker,
                timeout,
                now,
                fetch_variables,
                with_lease,
            } => {
                self.apply_activation_plan(
                    &mut log,
                    ActivationPlan {
                        job_keys,
                        worker,
                        timeout,
                        now,
                        options: crate::JobActivationOptions {
                            fetch_variables,
                            with_lease,
                        },
                        durable: true,
                    },
                );
            }

            Command::ExpireJobs { now } => {
                self.expire_job_locks(&mut log, now, None);
            }
            Command::ExpireJobsByDurability { now, durable } => {
                self.expire_job_locks(&mut log, now, Some(durable));
            }

            Command::TriggerTimers { now } => {
                // Fire every due timer (deterministic order by key). An
                // intermediate catch resumes its parked token; an interrupting
                // boundary timer interrupts its activity and routes out the
                // boundary event.
                let mut due: Vec<Key> = self
                    .state
                    .timers
                    .values()
                    .filter(|t| {
                        t.state == state::TimerState::Created
                            && t.due_at <= now
                            // A suspended instance makes no progress: its timers
                            // do not fire until it is resumed (Camunda parity).
                            && !self.instance_is_suspended(t.instance_key)
                    })
                    .map(|t| t.key)
                    .collect();
                due.sort_unstable();
                for timer_key in due {
                    // A boundary timer earlier in this batch may have interrupted
                    // an activity that disarmed this one; re-check it is still due.
                    let timer = match self.state.timers.get(&timer_key) {
                        Some(t) if t.state == state::TimerState::Created => t,
                        _ => continue,
                    };
                    let instance_key = timer.instance_key;
                    let element_instance_key = timer.element_instance_key;
                    let element_id = timer.element_id.clone();
                    let due_at = timer.due_at;
                    let kind = timer.kind.clone();

                    self.emit(
                        &mut log,
                        Event::TimerTriggered {
                            timer_key,
                            instance_key,
                            element_instance_key,
                            element_id: element_id.clone(),
                        },
                    );

                    match kind {
                        // Catch event: completing it resumes the token along its
                        // own outgoing flow.
                        state::TimerKind::IntermediateCatch => {
                            queue.push_back(Step::Complete {
                                instance_key,
                                element_instance_key,
                                element_id,
                            });
                        }
                        // Boundary timer: interrupt the attached activity (a
                        // service task or sub-process) and run the boundary
                        // event's outgoing flow. `element_instance_key`/
                        // `element_id` are the activity here.
                        state::TimerKind::InterruptingBoundary {
                            boundary_element_id,
                        } => {
                            let scope = self.scope_of(instance_key, element_instance_key);
                            self.interrupt_activity_via_boundary(
                                &mut log,
                                instance_key,
                                element_instance_key,
                                &element_id,
                            );
                            queue.push_back(Step::Activate {
                                instance_key,
                                element_id: boundary_element_id,
                                scope,
                                via: None,
                            });
                        }
                        // Non-interrupting boundary timer: leave the activity (and
                        // its job) running and spawn a parallel token along the
                        // boundary event's outgoing flow, in the activity's scope.
                        // A repeating (cycle) timer re-arms for the next interval.
                        state::TimerKind::NonInterruptingBoundary {
                            boundary_element_id,
                        } => {
                            queue.push_back(Step::Activate {
                                instance_key,
                                element_id: boundary_element_id.clone(),
                                scope: self.scope_of(instance_key, element_instance_key),
                                via: None,
                            });
                            if let Some(ElementKind::TimerBoundaryEvent {
                                duration_millis,
                                repeating: true,
                                ..
                            }) = self.element_kind(instance_key, &boundary_element_id)
                            {
                                let next_timer_key = self.mint_key();
                                // Re-evaluate a FEEL cycle against current vars so
                                // the next interval reflects any updated variable;
                                // a static cycle keeps its parsed interval.
                                let timer_def =
                                    self.timer_def_of(instance_key, &boundary_element_id);
                                let rearm_vars =
                                    self.variables_for_element(instance_key, element_instance_key);
                                let (next_due_at, _) = self.resolve_timer(
                                    &rearm_vars,
                                    timer_def.as_ref(),
                                    due_at,
                                    duration_millis,
                                );
                                self.emit(
                                    &mut log,
                                    Event::TimerCreated {
                                        timer_key: next_timer_key,
                                        instance_key,
                                        element_instance_key,
                                        element_id: element_id.clone(),
                                        due_at: next_due_at,
                                        kind: state::TimerKind::NonInterruptingBoundary {
                                            boundary_element_id,
                                        },
                                    },
                                );
                            }
                        }
                    }
                }

                // Process-level start timers: fire every due one (deterministic
                // by key), creating a new instance. A cycle re-arms for the next
                // interval; a one-shot is retained with no due time so it never
                // fires again.
                let mut due_starts: Vec<Key> = self
                    .state
                    .start_timers
                    .values()
                    .filter(|t| t.due_at.is_some_and(|d| d <= now))
                    .map(|t| t.timer_key)
                    .collect();
                due_starts.sort_unstable();
                for timer_key in due_starts {
                    let timer = match self.state.start_timers.get(&timer_key) {
                        Some(t) => t,
                        None => continue,
                    };
                    let process_id = timer.process_id.clone();
                    let start_element_id = timer.start_element_id.clone();
                    let due_at = match timer.due_at {
                        Some(d) => d,
                        None => continue,
                    };
                    let next_due_at = if timer.repeating {
                        Some(due_at.saturating_add(timer.interval_millis))
                    } else {
                        None
                    };
                    self.emit(
                        &mut log,
                        Event::ProcessStartTimerFired {
                            timer_key,
                            next_due_at,
                        },
                    );
                    self.start_or_dispatch_instance(
                        &mut log,
                        &mut queue,
                        process_id,
                        start_element_id,
                        HashMap::new(),
                        Vec::new(),
                        None,
                    );
                }
            }

            Command::FailJob {
                job_key,
                lease_token,
                retries,
                error_message,
            } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                if matches!(
                    job.state,
                    state::JobState::Completed
                        | state::JobState::Failed
                        | state::JobState::Errored
                        | state::JobState::Canceled
                ) {
                    return Err(EngineError::JobNotActive { job_key });
                }
                Self::validate_job_lease(job, lease_token.as_deref(), true)?;
                // Like completion, failing a job requires that it was activated
                // (unless lenient completion allows leader-local activation).
                if !self.lenient_completion && !job.activated {
                    return Err(EngineError::JobNotActivated { job_key });
                }
                let instance_key = job.instance_key;
                let element_instance_key = job.element_instance_key;
                let element_id = job.element_id.clone();
                let worker = job.attribution_worker();
                let retries = retries.max(0);

                self.emit(
                    &mut log,
                    Event::JobFailed {
                        job_key,
                        instance_key,
                        retries,
                        worker,
                        error_message: Some(limit_error_message(error_message.clone())),
                    },
                );
                // No retries left: park the job and raise an incident so the
                // instance stops making progress on this token.
                if retries == 0 {
                    let incident_key = self.mint_key();
                    self.emit(
                        &mut log,
                        Event::IncidentRaised {
                            incident_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                            kind: state::IncidentKind::JobNoRetries,
                            redrive: None,
                            reason: error_message,
                            job_key: Some(job_key),
                            created_at: self.now,
                        },
                    );
                }
            }

            Command::ThrowJobError {
                job_key,
                lease_token,
                error_code,
                error_message,
                variables,
            } => {
                let job = self
                    .state
                    .jobs
                    .get(&job_key)
                    .ok_or(EngineError::JobNotFound { job_key })?;
                if matches!(
                    job.state,
                    state::JobState::Completed
                        | state::JobState::Failed
                        | state::JobState::Errored
                        | state::JobState::Canceled
                ) {
                    return Err(EngineError::JobNotActive { job_key });
                }
                if !self.lenient_completion && !job.activated {
                    return Err(EngineError::JobNotActivated { job_key });
                }
                let instance_key = job.instance_key;
                let element_instance_key = job.element_instance_key;
                let task_element_id = job.element_id.clone();
                let worker = job.attribution_worker();

                Self::validate_job_lease(job, lease_token.as_deref(), true)?;
                // The job is consumed by the thrown error either way.
                self.emit(
                    &mut log,
                    Event::JobErrorThrown {
                        job_key,
                        instance_key,
                        error_code: error_code.clone(),
                        worker,
                        error_message: Some(limit_error_message(error_message.clone())),
                    },
                );

                match self.find_catching_error_boundary(
                    instance_key,
                    &task_element_id,
                    element_instance_key,
                    &error_code,
                ) {
                    // Caught: interrupt the catching activity (the throwing task
                    // itself, or an enclosing sub-process) and run the boundary
                    // event's error-handling path.
                    Some((boundary_id, caught_eik, caught_element_id)) => {
                        // Capture the catching activity's scope before completing
                        // it (completion clears its scope entry); the boundary
                        // event runs in that same scope.
                        let boundary_scope = self.scope_of(instance_key, caught_eik);
                        // When caught at an enclosing sub-process, terminate its
                        // whole inner scope (including the throwing task) first.
                        if caught_eik != element_instance_key {
                            self.terminate_subprocess_scope(&mut log, instance_key, caught_eik);
                        }
                        self.emit(
                            &mut log,
                            Event::ElementCompleting {
                                instance_key,
                                element_instance_key: caught_eik,
                                element_id: caught_element_id.clone(),
                            },
                        );
                        self.emit(
                            &mut log,
                            Event::ElementCompleted {
                                instance_key,
                                element_instance_key: caught_eik,
                                element_id: caught_element_id,
                            },
                        );
                        // An error boundary interrupting the activity also disarms
                        // any timer boundaries and message subscriptions on it.
                        for event in self.cancel_boundary_timers_on(caught_eik) {
                            self.emit(&mut log, event);
                        }
                        for event in self.cancel_boundary_message_subscriptions_on(caught_eik) {
                            self.emit(&mut log, event);
                        }
                        for event in self.cancel_boundary_signal_subscriptions_on(caught_eik) {
                            self.emit(&mut log, event);
                        }
                        for event in self.cancel_boundary_conditional_subscriptions_on(caught_eik) {
                            self.emit(&mut log, event);
                        }
                        // Seed the thrown error's variables at the local scope of
                        // the catch (the boundary event's scope), so the
                        // error-handling path downstream can read them (Camunda
                        // `JobErrorRequest.variables`).
                        if !variables.is_empty() {
                            for event in self.propagated_updates(
                                instance_key,
                                boundary_scope,
                                variables,
                                true,
                            ) {
                                self.emit(&mut log, event);
                            }
                        }
                        queue.push_back(Step::Activate {
                            instance_key,
                            element_id: boundary_id,
                            scope: boundary_scope,
                            via: None,
                        });
                    }
                    // Unhandled: the token parks on an incident.
                    None => {
                        let reason = if error_message.is_empty() {
                            format!("unhandled BPMN error '{error_code}'")
                        } else {
                            format!("unhandled BPMN error '{error_code}': {error_message}")
                        };
                        let incident_key = self.mint_key();
                        self.emit(
                            &mut log,
                            Event::IncidentRaised {
                                incident_key,
                                instance_key,
                                element_instance_key,
                                element_id: task_element_id,
                                kind: state::IncidentKind::UnhandledError,
                                redrive: None,
                                reason,
                                job_key: None,
                                created_at: self.now,
                            },
                        );
                    }
                }
            }

            Command::UpdateJobRetries {
                job_key,
                lease_token,
                retries,
                operation_reference,
            } => {
                self.update_job_properties(
                    &mut log,
                    job_key,
                    Some(retries),
                    None,
                    operation_reference,
                    lease_token.as_deref(),
                )?;
            }

            Command::UpdateJobTimeout {
                job_key,
                lease_token,
                timeout,
                operation_reference,
            } => {
                self.update_job_properties(
                    &mut log,
                    job_key,
                    None,
                    Some(timeout),
                    operation_reference,
                    lease_token.as_deref(),
                )?;
            }
            Command::UpdateJob {
                job_key,
                retries,
                timeout,
                operation_reference,
                lease_token,
            } => {
                self.update_job_properties(
                    &mut log,
                    job_key,
                    retries,
                    timeout,
                    operation_reference,
                    lease_token.as_deref(),
                )?;
            }

            Command::ResolveIncident {
                incident_key,
                operation_reference,
            } => {
                let incident = self
                    .state
                    .incidents
                    .get(&incident_key)
                    .ok_or(EngineError::IncidentNotFound { incident_key })?;
                // Only an active incident can be resolved; a retained (resolved)
                // record is history, not a live parked token.
                if incident.state != state::IncidentState::Active {
                    return Err(EngineError::IncidentNotResolvable {
                        incident_key,
                        reason: "incident is already resolved".to_string(),
                    });
                }
                let instance_key = incident.instance_key;
                let element_instance_key = incident.element_instance_key;
                let element_id = incident.element_id.clone();
                let kind = incident.kind;
                let redrive = incident.redrive.clone();
                let job_key = incident.job_key;
                // A job-incident can only be resolved once the parked job has
                // retries again; otherwise it would immediately re-fail.
                if let Some(job_key) = job_key {
                    let retries = self.state.jobs.get(&job_key).map_or(0, |j| j.retries);
                    if retries <= 0 {
                        return Err(EngineError::IncidentNotResolvable {
                            incident_key,
                            reason: format!(
                                "job {job_key} still has no retries; update its retries first"
                            ),
                        });
                    }
                }
                self.emit(
                    &mut log,
                    Event::IncidentResolved {
                        incident_key,
                        instance_key,
                        job_key,
                        resolved_at: self.now,
                        operation_reference,
                    },
                );
                // Resolution retries the failed work rather than merely clearing
                // the record. If the retry fails again a fresh incident is raised
                // by the same code paths that raised the original.
                match kind {
                    // Job exhausted its retries: the applier already returned the
                    // job to the activatable pool, so a worker retries it via the
                    // normal activate/complete path. Nothing more to enqueue.
                    state::IncidentKind::JobNoRetries => {}
                    // Exclusive gateway matched no flow, or a condition/decision/
                    // called-element expression failed to evaluate: re-evaluate the
                    // element against the (possibly updated) variables by re-driving
                    // `Complete`.
                    state::IncidentKind::NoMatchingSequenceFlow
                    | state::IncidentKind::ExpressionEvaluation
                    | state::IncidentKind::DecisionEvaluation
                    | state::IncidentKind::CalledElementError => {
                        // A message intermediate catch event whose correlation
                        // key failed to evaluate parks ACTIVATED with no
                        // subscription; re-driving `Complete` would advance the
                        // token past the wait (skipping the message). Instead
                        // re-open its subscription so the token keeps waiting.
                        if matches!(
                            self.element_kind(instance_key, &element_id),
                            Some(ElementKind::MessageIntermediateCatchEvent { .. })
                        ) {
                            queue.push_back(Step::ReopenCatch {
                                instance_key,
                                element_instance_key,
                                element_id,
                            });
                        } else if matches!(
                            redrive,
                            Some(state::IoMappingRedrive::CallActivitySpawn)
                                | Some(state::IoMappingRedrive::AdHocCallActivitySpawn { .. })
                        ) {
                            // A failed call-activity spawn (bad `=calledElement`
                            // expression, unknown callee, or depth overflow — issue
                            // #1159) parks the still-activated call activity / ad-hoc
                            // tool with no child. Re-driving `Complete` would advance
                            // the token (or, for an ad-hoc tool, complete it through
                            // `complete_adhoc_tool` with a manufactured all-null
                            // result) without ever instantiating the callee. Re-attempt
                            // the spawn instead, so resolving the incident (after the
                            // operator fixes the callee) actually creates the child.
                            // An ad-hoc call-activity tool carries its single-pass
                            // input projection on the redrive (#1176) so the respawn
                            // reuses it verbatim instead of re-projecting chained
                            // input mappings against the already-mutated child scope.
                            let preserved_seed = match &redrive {
                                Some(state::IoMappingRedrive::AdHocCallActivitySpawn {
                                    child_seed,
                                }) => Some(child_seed.clone()),
                                _ => None,
                            };
                            queue.push_back(Step::RetryCallActivitySpawn {
                                instance_key,
                                element_instance_key,
                                element_id,
                                preserved_seed,
                            });
                        } else if let Some(state::IoMappingRedrive::AdHocToolOutputCollection {
                            precomputed_output,
                        }) = &redrive
                        {
                            // An ad-hoc call-activity tool whose completion parked
                            // the output-collection *type* incident (#1176). Re-drive
                            // the tool's completion reusing the single-pass output
                            // projection captured on the redrive, rather than a plain
                            // `Step::Complete` that would re-evaluate the tool's
                            // (possibly chained) output mappings against the mutated
                            // child scope. Recover the container/inner element
                            // instances the leaf needs (the incident sits on the tool
                            // child); fall back to `Step::Complete` if the tool is no
                            // longer a live ad-hoc tool child.
                            match self.adhoc_tool_container_of(instance_key, element_instance_key) {
                                Some((container_key, inner_key)) => {
                                    queue.push_back(Step::CompleteAdHocCallActivityTool {
                                        instance_key,
                                        element_instance_key,
                                        element_id,
                                        container_key,
                                        inner_key,
                                        output_updates: precomputed_output.clone(),
                                    });
                                }
                                None => {
                                    queue.push_back(Step::Complete {
                                        instance_key,
                                        element_instance_key,
                                        element_id,
                                    });
                                }
                            }
                        } else {
                            queue.push_back(Step::Complete {
                                instance_key,
                                element_instance_key,
                                element_id,
                            });
                        }
                    }
                    // An *output* `zeebe:ioMapping` that failed at completion parks
                    // the element in the COMPLETING phase; re-driving `Complete`
                    // re-projects the now-fixed output mapping without re-running
                    // the element's behaviour — the same lifecycle as a
                    // script/decision output failure. This must NOT take the
                    // message-catch `ReopenCatch` branch above: an output-mapping
                    // failure occurs *after* the message was already correlated and
                    // consumed, so reopening the subscription would strand the token
                    // waiting for a *second* message instead of retrying the mapping.
                    // `ReopenCatch` is reserved for correlation-key (ACTIVATING)
                    // failures, which surface as `ExpressionEvaluation`.
                    // Uncaught business error: re-create a job for the still-active
                    // service task so a worker can attempt it again.
                    state::IncidentKind::UnhandledError => {
                        queue.push_back(Step::CreateJob {
                            instance_key,
                            element_instance_key,
                            element_id,
                        });
                    }
                    // A `zeebe:ioMapping` failure (input or output, any element
                    // type): recovery is **phase-driven** (#946). The re-drive is
                    // chosen from the [`state::IoMappingRedrive`] recorded on the
                    // incident — the element's lifecycle phase — rather than from
                    // the incident *kind*, so a single `IoMapping` taxonomy covers
                    // every ioMapping failure while each specialized path replays
                    // its correct context-preserving step (Zeebe parity: ioMapping
                    // failures re-drive uniformly by lifecycle phase — `ACTIVATING`
                    // re-applies inputs, `COMPLETING` re-applies outputs).
                    state::IncidentKind::IoMapping => {
                        let step: Option<Step> = match redrive {
                            // Mainstream input: re-run the activation body so the
                            // now-fixed inputs are re-applied before the element's
                            // behaviour runs (re-driving `Complete` would advance the
                            // token past the activity without ever running it).
                            Some(state::IoMappingRedrive::Activation) | None => {
                                Some(Step::RetryActivation {
                                    instance_key,
                                    element_instance_key,
                                    element_id,
                                })
                            }
                            // Output re-projection. A mainstream leaf element
                            // (service task, etc.) re-drives `Complete`, which
                            // re-evaluates its output mapping against its own
                            // still-live scope. A *sub-process*, however, projects
                            // its output mapping in the drain sweep
                            // (`complete_drained_subprocesses`), not `complete` —
                            // by the time a resolve runs, its inner scope is torn
                            // down, so `Complete` would re-evaluate against an empty
                            // view. Enqueue nothing for it: `IncidentResolved`
                            // touches the instance, so the drain sweep that runs
                            // after this command's queue empties re-detects the
                            // still-drained sub-process and re-projects the
                            // now-fixed output mapping.
                            Some(state::IoMappingRedrive::Completion) => {
                                if matches!(
                                    self.element_kind(instance_key, &element_id),
                                    Some(ElementKind::SubProcess { .. })
                                ) {
                                    // Re-driven by the drain sweep, not a step.
                                    None
                                } else {
                                    Some(Step::Complete {
                                        instance_key,
                                        element_instance_key,
                                        element_id,
                                    })
                                }
                            }
                            // MI child input: re-apply the child's inputs and
                            // re-enact its behaviour for the same child instance.
                            Some(state::IoMappingRedrive::MiChildActivation {
                                body_key,
                                index,
                            }) => Some(Step::RetryMiChildActivation {
                                instance_key,
                                element_id,
                                body_key,
                                child_key: element_instance_key,
                                index,
                            }),
                            // MI body input/collection: re-run the body fan-out.
                            Some(state::IoMappingRedrive::MiBodyActivation) => {
                                let scope = self.scope_of(instance_key, element_instance_key);
                                Some(Step::RetryMiBodyActivation {
                                    instance_key,
                                    element_id,
                                    body_key: element_instance_key,
                                    scope,
                                })
                            }
                            // MI body output aggregation: re-run body completion.
                            Some(state::IoMappingRedrive::MiBodyCompletion) => {
                                Some(Step::CompleteMiBody {
                                    instance_key,
                                    body_key: element_instance_key,
                                })
                            }
                            // Ad-hoc tool input: re-activate the tool child with its
                            // original seed variables (a clean retry).
                            Some(state::IoMappingRedrive::AdHocToolActivation {
                                element_id: tool_element_id,
                                variables,
                            }) => Some(Step::ActivateAdHocTool {
                                instance_key,
                                container_key: element_instance_key,
                                element_id: tool_element_id,
                                variables,
                            }),
                            // Call-activity input: re-attempt the child-process spawn
                            // (boundary events stay armed; not re-emitted).
                            Some(state::IoMappingRedrive::CallActivitySpawn) => {
                                Some(Step::RetryCallActivitySpawn {
                                    instance_key,
                                    element_instance_key,
                                    element_id,
                                    preserved_seed: None,
                                })
                            }
                            // Ad-hoc call-activity tool spawn (#1176): re-attempt the
                            // spawn reusing the tool's preserved single-pass input
                            // projection. (Spawn incidents surface as
                            // `ExpressionEvaluation`/`CalledElementError`, handled
                            // above; this arm keeps the `IoMapping` match exhaustive.)
                            Some(state::IoMappingRedrive::AdHocCallActivitySpawn {
                                child_seed,
                            }) => Some(Step::RetryCallActivitySpawn {
                                instance_key,
                                element_instance_key,
                                element_id,
                                preserved_seed: Some(child_seed),
                            }),
                            // Ad-hoc tool output-collection type incident (#1176):
                            // re-drive the tool completion reusing the preserved
                            // single-pass output projection. (Surfaces as
                            // `ExpressionEvaluation`, handled above; this arm keeps
                            // the `IoMapping` match exhaustive.)
                            Some(state::IoMappingRedrive::AdHocToolOutputCollection {
                                precomputed_output,
                            }) => self
                                .adhoc_tool_container_of(instance_key, element_instance_key)
                                .map(|(container_key, inner_key)| {
                                    Step::CompleteAdHocCallActivityTool {
                                        instance_key,
                                        element_instance_key,
                                        element_id: element_id.clone(),
                                        container_key,
                                        inner_key,
                                        output_updates: precomputed_output.clone(),
                                    }
                                })
                                .or(Some(Step::Complete {
                                    instance_key,
                                    element_instance_key,
                                    element_id,
                                })),
                            // Call-activity output: re-project the captured child
                            // result through the output mappings and complete.
                            Some(state::IoMappingRedrive::CallActivityCompletion {
                                child_variables,
                            }) => Some(Step::CompleteCallActivity {
                                instance_key,
                                element_instance_key,
                                element_id,
                                child_variables,
                            }),
                        };
                        if let Some(step) = step {
                            queue.push_back(step);
                        }
                    }
                }
            }

            Command::SetVariables {
                scope_key,
                variables,
                local,
            } => {
                let instance_key = self
                    .resolve_scope(scope_key)
                    .ok_or(EngineError::ScopeNotFound { scope_key })?;
                if !variables.is_empty() {
                    // Resolve the requested scope within the instance and write:
                    // `local` keeps the values in that scope; otherwise they
                    // propagate to the nearest ancestor defining each name, else
                    // root. A root-scoped (or root-only-instance) write collapses
                    // to the flat `VariablesUpdated`.
                    for event in self.propagated_updates(instance_key, scope_key, variables, local)
                    {
                        self.emit(&mut log, event);
                    }
                }
            }

            Command::CorrelateMessage {
                message_name,
                correlation_key,
                variables,
                business_id,
            } => {
                check_business_id(business_id.as_deref())?;
                // Always mint a message key (Zeebe records every published
                // message); it is returned to the host and, carried on the
                // MessagePublished event, restores the key generator on replay.
                let message_key = self.mint_key();
                self.emit(
                    &mut log,
                    Event::MessagePublished {
                        message_key,
                        message_name: message_name.clone(),
                        correlation_key: correlation_key.clone(),
                    },
                );

                // Correlate to every matching open subscription, deterministic by
                // subscription key. Messages are not buffered: with no match the
                // message is simply dropped.
                let mut matched: Vec<Key> = self
                    .state
                    .message_subscriptions
                    .values()
                    .filter(|s| {
                        s.state == state::MessageSubscriptionState::Open
                            && s.message_name == message_name
                            && s.correlation_key == correlation_key
                    })
                    .map(|s| s.key)
                    .collect();
                matched.sort_unstable();

                // Zeebe prefers an already-open subscription on a running
                // instance over creating a fresh instance from a message start
                // event. If any matching subscription is still open when this
                // message arrives, it takes precedence and the message-start-event
                // instance creation below is suppressed (issue #1156).
                let mut correlated_to_subscription = false;

                for subscription_key in matched {
                    // A boundary correlation earlier in this batch may have
                    // interrupted an activity that cancelled this subscription;
                    // re-check it is still open.
                    let subscription = match self.state.message_subscriptions.get(&subscription_key)
                    {
                        Some(s) if s.state == state::MessageSubscriptionState::Open => s,
                        _ => continue,
                    };
                    // An open subscription for this message name and correlation
                    // key exists on a running instance: it claims the message, so
                    // no new instance is started from a message start event.
                    correlated_to_subscription = true;
                    let instance_key = subscription.instance_key;
                    let element_instance_key = subscription.element_instance_key;
                    let element_id = subscription.element_id.clone();
                    let kind = subscription.kind.clone();

                    if state::partition_of(instance_key) == self.partition_id {
                        // A suspended instance makes no progress: it does not
                        // correlate messages (Camunda parity with its gated jobs
                        // and timers). Because the message model is unbuffered
                        // (see the note above), the correlation is *dropped* — the
                        // subscription stays `Open` for a future message but this
                        // one does not re-correlate on resume. Drop-on-suspend is
                        // the documented suspension semantics for messages.
                        if self.instance_is_suspended(instance_key) {
                            continue;
                        }
                        // The instance lives on this partition: correlate and
                        // advance its token inline. This is the only path a
                        // single-partition host ever takes, so its log is
                        // byte-identical to the pre-placement engine.
                        self.advance_correlated_token(
                            &mut log,
                            &mut queue,
                            subscription_key,
                            message_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                            kind,
                            &variables,
                        );
                    } else {
                        // The instance lives on another partition: settle the
                        // canonical subscription here and hand the token-advance to
                        // the host, which routes a `CorrelateMessageSubscription`
                        // continuation to `partition_of(instance_key)`.
                        self.emit(
                            &mut log,
                            Event::RemoteMessageCorrelation {
                                subscription_key,
                                message_key,
                                instance_key,
                                element_instance_key,
                                element_id,
                                kind,
                                variables: variables.clone(),
                            },
                        );
                    }
                }

                // Message start events: a matching message also creates a new
                // instance of every process subscribed at the process level —
                // but only when the message was not already claimed by an open
                // subscription on a running instance. An already-open
                // subscription takes precedence over starting a fresh instance
                // for the same publish (Zeebe parity, issue #1156).
                // Deterministic by process-definition key.
                // The message's variables seed the new instance.
                let mut started: Vec<(String, ElementId)> = if correlated_to_subscription {
                    Vec::new()
                } else {
                    self.state
                        .message_start_subscriptions
                        .values()
                        .filter(|s| s.message_name == message_name)
                        .map(|s| (s.process_id.clone(), s.start_element_id.clone()))
                        .collect()
                };
                started.sort_unstable();
                for (process_id, start_element_id) in started {
                    self.start_or_dispatch_instance(
                        &mut log,
                        &mut queue,
                        process_id,
                        start_element_id,
                        variables.clone(),
                        Vec::new(),
                        business_id.clone(),
                    );
                }
            }

            Command::BroadcastSignal {
                signal_name,
                variables,
            } => {
                // Mint a signal key (returned to the host; carried on the
                // SignalBroadcast event it restores the key generator on replay).
                let signal_key = self.mint_key();
                self.emit(
                    &mut log,
                    Event::SignalBroadcast {
                        signal_key,
                        signal_name: signal_name.clone(),
                    },
                );

                // Correlate to every matching open subscription, deterministic by
                // subscription key. Signals are not buffered: with no match the
                // signal is simply dropped.
                let mut matched: Vec<Key> = self
                    .state
                    .signal_subscriptions
                    .values()
                    .filter(|s| {
                        s.state == state::MessageSubscriptionState::Open
                            && s.signal_name == signal_name
                    })
                    .map(|s| s.key)
                    .collect();
                matched.sort_unstable();

                for subscription_key in matched {
                    // An earlier boundary correlation in this batch may have
                    // interrupted an activity that cancelled this subscription;
                    // re-check it is still open.
                    let subscription = match self.state.signal_subscriptions.get(&subscription_key)
                    {
                        Some(s) if s.state == state::MessageSubscriptionState::Open => s,
                        _ => continue,
                    };
                    let instance_key = subscription.instance_key;
                    let element_instance_key = subscription.element_instance_key;
                    let element_id = subscription.element_id.clone();
                    let kind = subscription.kind.clone();
                    self.advance_signal_correlated_token(
                        &mut log,
                        &mut queue,
                        subscription_key,
                        signal_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        kind,
                        &variables,
                    );
                }
            }

            // These three commands are routed by the host between the instance
            // partition (where a token waits) and the message partition
            // (`hash(correlation_key)`, where the canonical subscription lives and
            // where published messages correlate). A single-partition host never
            // emits the events that drive them, so they only fire in a cluster.
            Command::OpenMessageSubscription {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
                message_name,
                correlation_key,
                kind,
            } => {
                // Idempotent: re-delivering an open for a subscription we already
                // hold (at-least-once retry, or a duplicate) is a no-op.
                if !self
                    .state
                    .message_subscriptions
                    .contains_key(&subscription_key)
                {
                    self.emit(
                        &mut log,
                        Event::MessageSubscriptionCreated {
                            subscription_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                            message_name,
                            correlation_key,
                            kind,
                        },
                    );
                }
            }

            Command::CorrelateMessageSubscription {
                subscription_key,
                message_key,
                instance_key,
                element_instance_key,
                element_id,
                kind,
                variables,
            } => {
                // Advance only while the local record is still `Opening` (a parked
                // token). Once it settles to `Correlated`/`Canceled` — or the
                // instance is gone — a redelivered continuation is safely ignored,
                // which is what makes the routed delivery at-least-once safe. A
                // non-interrupting boundary keeps its `Opening` record open, so
                // every routed message spawns another token.
                //
                // A suspended instance makes no progress (Camunda parity with its
                // gated jobs and timers): the routed correlation is *dropped*. The
                // message model is unbuffered, so a dropped correlation does not
                // re-correlate on resume — drop-on-suspend is the documented
                // suspension semantics for messages.
                let advance = matches!(
                    self.state
                        .message_subscriptions
                        .get(&subscription_key)
                        .map(|s| s.state),
                    Some(state::MessageSubscriptionState::Opening)
                ) && !self.instance_is_suspended(instance_key);
                if advance {
                    self.advance_correlated_token(
                        &mut log,
                        &mut queue,
                        subscription_key,
                        message_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        kind,
                        &variables,
                    );
                }
            }

            Command::CloseMessageSubscription {
                subscription_key,
                instance_key,
                element_instance_key,
                element_id,
            } => {
                // Disarm the canonical subscription because the instance partition
                // tore down the waiting element. Idempotent: only an open
                // subscription is cancelled.
                let open = matches!(
                    self.state
                        .message_subscriptions
                        .get(&subscription_key)
                        .map(|s| s.state),
                    Some(state::MessageSubscriptionState::Open)
                );
                if open {
                    self.emit(
                        &mut log,
                        Event::MessageSubscriptionCanceled {
                            subscription_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                        },
                    );
                }
            }

            Command::CancelInstance { instance_key } => {
                // Only an active instance can be cancelled. An unknown key, or one
                // that has already completed/terminated, is rejected so the caller
                // gets a clean 404.
                match self.state.instances.get(&instance_key) {
                    Some(instance) if instance.state == ProcessInstanceState::Active => {}
                    _ => return Err(EngineError::InstanceNotFound { instance_key }),
                }

                // Discard every token: cancel the instance's in-play jobs, armed
                // timers and open message subscriptions. Collected and ordered by
                // key first so the event sequence is deterministic.
                let mut jobs: Vec<&state::Job> = self
                    .state
                    .jobs
                    .values()
                    .filter(|j| {
                        j.instance_key == instance_key
                            && matches!(
                                j.state,
                                state::JobState::Created
                                    | state::JobState::Activated
                                    | state::JobState::Failed
                            )
                    })
                    .collect();
                jobs.sort_unstable_by_key(|j| j.key);
                let job_cancels: Vec<Event> = jobs
                    .iter()
                    .map(|j| Event::JobCanceled {
                        job_key: j.key,
                        instance_key,
                    })
                    .collect();

                let mut timers: Vec<&state::Timer> = self
                    .state
                    .timers
                    .values()
                    .filter(|t| {
                        t.instance_key == instance_key && t.state == state::TimerState::Created
                    })
                    .collect();
                timers.sort_unstable_by_key(|t| t.key);
                let timer_cancels: Vec<Event> = timers
                    .iter()
                    .map(|t| Event::TimerCanceled {
                        timer_key: t.key,
                        instance_key,
                        element_instance_key: t.element_instance_key,
                        element_id: t.element_id.clone(),
                    })
                    .collect();

                let mut subs: Vec<&state::MessageSubscription> = self
                    .state
                    .message_subscriptions
                    .values()
                    .filter(|s| {
                        s.instance_key == instance_key
                            && matches!(
                                s.state,
                                state::MessageSubscriptionState::Open
                                    | state::MessageSubscriptionState::Opening
                            )
                    })
                    .collect();
                subs.sort_unstable_by_key(|s| s.key);
                let sub_cancels: Vec<Event> = subs
                    .iter()
                    .map(|s| Self::disarm_subscription_event(s))
                    .collect();

                let mut sig_subs: Vec<&state::SignalSubscription> = self
                    .state
                    .signal_subscriptions
                    .values()
                    .filter(|s| {
                        s.instance_key == instance_key
                            && s.state == state::MessageSubscriptionState::Open
                    })
                    .collect();
                sig_subs.sort_unstable_by_key(|s| s.key);
                let sig_sub_cancels: Vec<Event> = sig_subs
                    .iter()
                    .map(|s| Event::SignalSubscriptionCanceled {
                        subscription_key: s.key,
                        instance_key,
                        element_instance_key: s.element_instance_key,
                        element_id: s.element_id.clone(),
                    })
                    .collect();

                let mut cond_subs: Vec<&state::ConditionalSubscription> = self
                    .state
                    .conditional_subscriptions
                    .values()
                    .filter(|s| {
                        s.instance_key == instance_key
                            && s.state == state::MessageSubscriptionState::Open
                    })
                    .collect();
                cond_subs.sort_unstable_by_key(|s| s.key);
                let cond_sub_cancels: Vec<Event> = cond_subs
                    .iter()
                    .map(|s| Event::ConditionalSubscriptionCanceled {
                        subscription_key: s.key,
                        instance_key,
                        element_instance_key: s.element_instance_key,
                        element_id: s.element_id.clone(),
                    })
                    .collect();

                let mut user_tasks: Vec<&state::UserTask> = self
                    .state
                    .user_tasks
                    .values()
                    .filter(|t| {
                        t.instance_key == instance_key && t.state == state::UserTaskState::Created
                    })
                    .collect();
                user_tasks.sort_unstable_by_key(|t| t.key);
                // Split user tasks into those that must run a `canceling` listener
                // chain before cancellation (deferred) and those cancelled at once
                // (ADR 0037 §6). A task already deferring another transition is
                // cancelled immediately (its in-flight listener job is cancelled
                // with the other jobs above). Owned tuples so we can call
                // `task_listeners_of` (which borrows `self`) after this.
                let user_task_infos: Vec<(Key, Key, ElementId, bool)> = user_tasks
                    .iter()
                    .map(|t| {
                        (
                            t.key,
                            t.element_instance_key,
                            t.element_id.clone(),
                            t.pending.is_none(),
                        )
                    })
                    .collect();
                let mut immediate_user_task_cancels: Vec<Event> = Vec::new();
                let mut canceling_starts: Vec<(Key, Key, ElementId, crate::model::TaskListener)> =
                    Vec::new();
                for (key, element_instance_key, element_id, no_pending) in user_task_infos {
                    let canceling = if no_pending {
                        self.task_listeners_of(
                            instance_key,
                            &element_id,
                            crate::model::TaskListenerEventType::Canceling,
                        )
                    } else {
                        Vec::new()
                    };
                    if let Some(first) = canceling.first().cloned() {
                        canceling_starts.push((key, element_instance_key, element_id, first));
                    } else {
                        immediate_user_task_cancels.push(Event::UserTaskCanceled {
                            user_task_key: key,
                            instance_key,
                        });
                    }
                }

                for event in job_cancels {
                    self.emit(&mut log, event);
                }
                for event in timer_cancels {
                    self.emit(&mut log, event);
                }
                for event in sub_cancels {
                    self.emit(&mut log, event);
                }
                for event in sig_sub_cancels {
                    self.emit(&mut log, event);
                }
                for event in cond_sub_cancels {
                    self.emit(&mut log, event);
                }
                for event in immediate_user_task_cancels {
                    self.emit(&mut log, event);
                }
                if canceling_starts.is_empty() {
                    // No canceling listeners: terminate synchronously, exactly as
                    // the pre-task-listener engine did (byte-identical).
                    self.emit(&mut log, Event::ProcessInstanceTerminated { instance_key });
                } else {
                    // Defer termination: run each user task's canceling chain; the
                    // last one to drain emits `ProcessInstanceTerminated`.
                    self.emit(&mut log, Event::ProcessInstanceTerminating { instance_key });
                    for (key, element_instance_key, element_id, first) in canceling_starts {
                        let pending = state::PendingUserTaskTransition {
                            event_type: crate::model::TaskListenerEventType::Canceling,
                            assignee: None,
                            update: None,
                            variables: std::collections::HashMap::new(),
                            corrections: crate::model::UserTaskCorrections::default(),
                        };
                        for event in self.start_task_listener_chain(
                            key,
                            instance_key,
                            element_instance_key,
                            &element_id,
                            crate::model::TaskListenerEventType::Canceling,
                            pending,
                            &first,
                        ) {
                            self.emit(&mut log, event);
                        }
                    }
                }
            }

            Command::SuspendInstance { instance_key } => {
                // Camunda parity: only an `Active` instance can be suspended.
                // Suspending an already-suspended instance is an idempotent
                // no-op; an unknown key is a clean 404; any terminal state is an
                // illegal transition.
                match self.state.instances.get(&instance_key) {
                    Some(instance) => match instance.state {
                        ProcessInstanceState::Active => {
                            self.emit(
                                &mut log,
                                Event::ProcessInstanceSuspended {
                                    instance_key,
                                    at: now,
                                },
                            );
                        }
                        ProcessInstanceState::Suspended => {
                            // Already suspended — no-op.
                        }
                        other => {
                            return Err(EngineError::InstanceTransitionInvalid {
                                instance_key,
                                from: other.as_str(),
                                to: "SUSPENDED",
                            })
                        }
                    },
                    None => return Err(EngineError::InstanceNotFound { instance_key }),
                }
            }

            Command::ResumeInstance { instance_key } => {
                // Camunda parity: only a `Suspended` instance can be resumed.
                // Resuming an already-active instance is an idempotent no-op; an
                // unknown key is a clean 404; any terminal state is illegal.
                match self.state.instances.get(&instance_key) {
                    Some(instance) => match instance.state {
                        ProcessInstanceState::Suspended => {
                            self.emit(&mut log, Event::ProcessInstanceResumed { instance_key });
                        }
                        ProcessInstanceState::Active => {
                            // Already active — no-op.
                        }
                        other => {
                            return Err(EngineError::InstanceTransitionInvalid {
                                instance_key,
                                from: other.as_str(),
                                to: "ACTIVE",
                            })
                        }
                    },
                    None => return Err(EngineError::InstanceNotFound { instance_key }),
                }
            }

            Command::MigrateInstance {
                instance_key,
                target_process_definition_key,
                mapping_instructions,
            } => {
                // 1. Only an active instance can be migrated; unknown or finished
                //    is a clean 404 (mirrors CancelInstance).
                let source_process_id = match self.state.instances.get(&instance_key) {
                    Some(i) if i.state == ProcessInstanceState::Active => i.process_id.clone(),
                    _ => return Err(EngineError::InstanceNotFound { instance_key }),
                };

                // 2. The target definition must be deployed. `state.processes` is
                //    keyed by process id and retains only the latest version, so
                //    the target key must belong to a currently-deployed latest
                //    version (Phase 1 limitation, matches the model).
                let target = self
                    .state
                    .processes
                    .values()
                    .find(|dp| dp.key == target_process_definition_key)
                    .ok_or(EngineError::TargetProcessDefinitionNotFound {
                        process_definition_key: target_process_definition_key,
                    })?;
                let target_process_id = target.definition.id.clone();

                // The instance's current definition is the source of truth for its
                // active elements' types (only the latest version is retained).
                let source_def = self.state.processes.get(&source_process_id).ok_or(
                    EngineError::ProcessNotFound {
                        process_id: source_process_id.clone(),
                    },
                )?;

                // 3. No duplicate source ids; every mapped source/target element
                //    exists; each mapped pair keeps the same BPMN type (compared
                //    by discriminant only, so a service task may map to one with a
                //    different job type — Zeebe parity).
                let mut mapped: HashMap<String, String> =
                    HashMap::with_capacity(mapping_instructions.len());
                for (src, tgt) in &mapping_instructions {
                    if mapped.contains_key(src) {
                        return Err(EngineError::DuplicateMappingSourceElement {
                            instance_key,
                            element_id: src.clone(),
                        });
                    }
                    let src_el = source_def.definition.element(src).ok_or_else(|| {
                        EngineError::MappingSourceElementNotFound {
                            instance_key,
                            element_id: src.clone(),
                        }
                    })?;
                    let tgt_el = target.definition.element(tgt).ok_or_else(|| {
                        EngineError::MappingTargetElementNotFound {
                            target_process_definition_key,
                            element_id: tgt.clone(),
                        }
                    })?;
                    if std::mem::discriminant(&src_el.kind) != std::mem::discriminant(&tgt_el.kind)
                    {
                        return Err(EngineError::MappedElementTypeChanged {
                            instance_key,
                            source_element_id: src.clone(),
                            target_element_id: tgt.clone(),
                        });
                    }
                    mapped.insert(src.clone(), tgt.clone());
                }

                // 4. Reject element classes this phase cannot remap safely.
                //    Two sources of "unsupported":
                //    (a) an active element instance whose own kind is a
                //        sub-process, call activity, or event-based gateway (the
                //        token rests directly on it, so it appears in `active`);
                //    (b) an armed boundary event — its runtime (timer/subscription)
                //        is stored against the *host* activity's element id with a
                //        boundary `kind`, so the boundary element id lives inside
                //        that kind rather than in `active`.
                //    Zeebe rejects several of these as "not supported yet";
                //    reproducing the rejection is parity.
                let instance = self
                    .state
                    .instances
                    .get(&instance_key)
                    .expect("instance existence checked above");
                let mut active_ids: Vec<String> = instance.active.values().cloned().collect();
                active_ids.sort();
                active_ids.dedup();
                for eid in &active_ids {
                    if let Some(el) = source_def.definition.element(eid) {
                        if let Some(reason) = unsupported_migration_reason(&el.kind) {
                            return Err(EngineError::UnsupportedMigration {
                                instance_key,
                                element_id: eid.clone(),
                                reason: reason.to_string(),
                            });
                        }
                    }
                }
                let boundary_reason = "boundary events are not migratable yet";
                let mut armed_boundaries: Vec<String> = Vec::new();
                for t in self.state.timers.values() {
                    if t.instance_key == instance_key {
                        match &t.kind {
                            state::TimerKind::InterruptingBoundary {
                                boundary_element_id,
                            }
                            | state::TimerKind::NonInterruptingBoundary {
                                boundary_element_id,
                            } => armed_boundaries.push(boundary_element_id.clone()),
                            _ => {}
                        }
                    }
                }
                let boundary_kind = |kind: &state::MessageSubscriptionKind| match kind {
                    state::MessageSubscriptionKind::InterruptingBoundary {
                        boundary_element_id,
                    }
                    | state::MessageSubscriptionKind::NonInterruptingBoundary {
                        boundary_element_id,
                    } => Some(boundary_element_id.clone()),
                    _ => None,
                };
                for s in self.state.message_subscriptions.values() {
                    if s.instance_key == instance_key {
                        if let Some(id) = boundary_kind(&s.kind) {
                            armed_boundaries.push(id);
                        }
                    }
                }
                for s in self.state.signal_subscriptions.values() {
                    if s.instance_key == instance_key {
                        if let Some(id) = boundary_kind(&s.kind) {
                            armed_boundaries.push(id);
                        }
                    }
                }
                for s in self.state.conditional_subscriptions.values() {
                    if s.instance_key == instance_key {
                        if let Some(id) = boundary_kind(&s.kind) {
                            armed_boundaries.push(id);
                        }
                    }
                }
                if let Some(boundary_element_id) = armed_boundaries.into_iter().min() {
                    return Err(EngineError::UnsupportedMigration {
                        instance_key,
                        element_id: boundary_element_id,
                        reason: boundary_reason.to_string(),
                    });
                }

                //    (c) an active multi-instance body or ad-hoc sub-process
                //        container. Their per-element bookkeeping lives in
                //        `ProcessInstance.multi_instances` / `adhoc_instances`,
                //        which the applier (`state::apply`) does not remap, so
                //        migrating one would leave that bookkeeping pointing at
                //        the source element id and desync engine runtime. Zeebe
                //        rejects these as "not supported yet"; reproducing the
                //        rejection is parity.
                let mut unremappable: Vec<String> = instance
                    .multi_instances
                    .values()
                    .map(|mi| mi.element_id.clone())
                    .chain(
                        instance
                            .adhoc_instances
                            .values()
                            .map(|adhoc| adhoc.element_id.clone()),
                    )
                    .collect();
                unremappable.sort();
                if let Some(element_id) = unremappable.into_iter().next() {
                    return Err(EngineError::UnsupportedMigration {
                        instance_key,
                        element_id,
                        reason: "multi-instance and ad-hoc sub-process activities are not \
                                 migratable yet"
                            .to_string(),
                    });
                }

                //    (d) Zeebe's "flow scope unchanged" precondition, enforced
                //        categorically. The applier (`state::apply`) re-points
                //        element ids but deliberately does NOT remap the scope
                //        tree (`scopes` / `scope_parents` / `scope_variables`),
                //        so an active element sitting inside a non-root flow
                //        scope cannot be migrated without desyncing that tree.
                //        Every element type that can own such a scope today is
                //        already rejected above — an active embedded sub-process
                //        appears in `active` (a), and multi-instance / ad-hoc
                //        bodies via their bookkeeping maps (c) — so in the current
                //        supported subset this is a defense-in-depth backstop.
                //        Keeping it as an explicit precondition (rather than an
                //        emergent property of those specific guards) prevents a
                //        future scope-owning element type from silently slipping a
                //        nested-scope token past them into the non-remapping
                //        applier. `instance.scopes` keys are exactly the active
                //        element instances that live in a non-root scope.
                let mut scoped_active: Vec<String> = instance
                    .scopes
                    .keys()
                    .filter_map(|element_instance_key| {
                        instance.active.get(element_instance_key).cloned()
                    })
                    .collect();
                scoped_active.sort();
                scoped_active.dedup();
                if let Some(element_id) = scoped_active.into_iter().next() {
                    return Err(EngineError::UnsupportedMigration {
                        instance_key,
                        element_id,
                        reason: "elements inside a nested flow scope are not migratable yet"
                            .to_string(),
                    });
                }

                // 5. Every active element instance must have a mapping.
                for eid in &active_ids {
                    if !mapped.contains_key(eid) {
                        return Err(EngineError::UnmappedActiveElement {
                            instance_key,
                            element_id: eid.clone(),
                        });
                    }
                }

                // 5b. An already-*open* parallel-gateway join (a token has
                //     arrived on some but not all of its incoming flows) carries
                //     durable count-vs-threshold state: `join_flow_arrivals`
                //     (and, for pre-#1233 journals, `join_counts`) holds the
                //     tokens counted per incoming flow, but the set of flows it
                //     must cover is read *live from the definition* at fire time
                //     (`activate_join` calls `incoming_count`, which
                //     resolves against the instance's current process). Migration
                //     swaps that definition, so mapping an open join onto a target
                //     gateway with a different incoming-flow count silently
                //     re-interprets the partial count against a new threshold:
                //     the join can early-fire (target arity <= tokens already
                //     arrived) or deadlock (target arity is unreachable because
                //     the missing source flows do not exist in the target). The
                //     discriminant-only type check in step 3 does not catch this
                //     — both are `ParallelGateway`. Require incoming-flow-count
                //     parity for every open join. This is the general guard for
                //     the failure-mode class "durable count state whose threshold
                //     lives in the definition being migrated away"; parallel-join
                //     is the only such element today. Only *open* joins are
                //     guarded: a join with no arrivals yet has no durable count to
                //     re-interpret, so it may safely adopt the target's arity.
                let instance = self
                    .state
                    .instances
                    .get(&instance_key)
                    .expect("instance existence checked above");
                let mut open_joins: Vec<String> = instance.join_instances.keys().cloned().collect();
                open_joins.sort();
                for src in open_joins {
                    let join_kind = source_def.definition.elements.get(&src).map(|e| &e.kind);
                    let is_parallel_join = matches!(join_kind, Some(ElementKind::ParallelGateway));
                    let is_inclusive_join =
                        matches!(join_kind, Some(ElementKind::InclusiveGateway));
                    if !is_parallel_join && !is_inclusive_join {
                        continue;
                    }
                    let tgt = mapped
                        .get(&src)
                        .expect("an open join is active, so step 5 guarantees it is mapped");
                    // An inclusive join accepts when its taken flows cover its
                    // incoming flows, or when no active path can still reach it
                    // (#1241). Every *identified* arrival must name an incoming
                    // flow of the target (the per-flow check below), so the
                    // covering test cannot early-fire under a different arity —
                    // an open inclusive join with only identified arrivals may
                    // migrate across an arity change. An *unidentified* arrival
                    // (`join_counts`) counts as one taken flow of the arity it
                    // was recorded under, so it needs parity just like a
                    // parallel join.
                    let has_unidentified = instance.join_counts.get(&src).is_some_and(|n| *n > 0);
                    if is_parallel_join || has_unidentified {
                        let source_incoming_count = source_def.definition.incoming_count(&src);
                        let target_incoming_count = target.definition.incoming_count(tgt);
                        if source_incoming_count != target_incoming_count {
                            return Err(EngineError::MigratedParallelJoinArityChanged {
                                instance_key,
                                source_element_id: src,
                                target_element_id: tgt.clone(),
                                source_incoming_count,
                                target_incoming_count,
                            });
                        }
                    }
                    // Each flow a parallel or inclusive join has counted a token
                    // on must still be an incoming flow of the target join (after
                    // the applier renames its source by the same mapping). A
                    // parallel join would otherwise count a flow that no longer
                    // exists and fire early; an inclusive join would carry a token
                    // no flow can consume. Zeebe requires a target for every taken
                    // sequence flow into either gateway
                    // (`requireNonNullTargetSequenceFlowId`, #1233, #1237).
                    //
                    // Unidentified arrivals (`join_counts`: a pre-#1233 journal,
                    // or a modification that activated the join directly) name
                    // no flow, so there is nothing to look up. Each counts as
                    // one taken flow, exactly as before #1233, and the arity
                    // parity check above keeps that reading valid in the target.
                    // Rejecting them instead would strand every half-open
                    // legacy join on its old definition.
                    if let Some(arrivals) = instance.join_flow_arrivals.get(&src) {
                        for (flow, _) in arrivals.iter() {
                            let migrated = IncomingFlow {
                                from: mapped.get(&flow.from).unwrap_or(&flow.from).clone(),
                                ordinal: flow.ordinal,
                            };
                            if !target.definition.has_incoming_flow(tgt, &migrated) {
                                return Err(EngineError::MigratedJoinFlowMissing {
                                    instance_key,
                                    source_element_id: src,
                                    target_element_id: tgt.clone(),
                                    flow_source_element_id: flow.from.clone(),
                                    flow_ordinal: flow.ordinal,
                                });
                            }
                        }
                    }
                }

                // 6. All preconditions hold: emit the migration fact. The applier
                //    rewrites the instance's process id and re-points every active
                //    element instance and its attached runtime.
                self.emit(
                    &mut log,
                    Event::ProcessInstanceMigrated {
                        instance_key,
                        target_process_id,
                        target_process_definition_key,
                        element_mappings: mapping_instructions,
                    },
                );
            }

            Command::ModifyInstance {
                instance_key,
                activate_instructions,
                terminate_instructions,
            } => {
                // Only an active instance can be modified (Zeebe parity).
                match self.state.instances.get(&instance_key) {
                    Some(instance) if instance.state == ProcessInstanceState::Active => {}
                    _ => return Err(EngineError::InstanceNotFound { instance_key }),
                }

                // Validate up front so the command is all-or-nothing: every
                // activate element id must exist in the process definition and
                // every terminate key must be a currently-active element instance.
                for a in &activate_instructions {
                    let exists = self
                        .process_of_instance(instance_key)
                        .map(|p| p.element(&a.element_id).is_some())
                        .unwrap_or(false);
                    if !exists {
                        return Err(EngineError::ElementNotFound {
                            instance_key,
                            element_id: a.element_id.clone(),
                        });
                    }
                }
                for &eik in &terminate_instructions {
                    let active = self
                        .state
                        .instances
                        .get(&instance_key)
                        .map(|i| i.active.contains_key(&eik))
                        .unwrap_or(false);
                    if !active {
                        return Err(EngineError::ElementInstanceNotFound {
                            instance_key,
                            element_instance_key: eik,
                        });
                    }
                }

                // Apply terminations first (deterministic key order), then merge
                // any global variables and queue the activations at the process
                // root scope.
                let mut terminate: Vec<Key> = terminate_instructions;
                terminate.sort_unstable();
                terminate.dedup();
                for eik in terminate {
                    // The eik was validated against `instance.active` above, so
                    // its element id must resolve. Skip defensively rather than
                    // emitting a termination with an empty element_id, which
                    // would corrupt downstream element aggregates.
                    let Some(element_id) = self.element_id_of_instance(instance_key, eik) else {
                        continue;
                    };
                    self.terminate_element_instance(&mut log, instance_key, eik, &element_id);
                }

                for a in &activate_instructions {
                    if !a.variables.is_empty() {
                        for event in self.propagated_updates(
                            instance_key,
                            instance_key,
                            a.variables.clone(),
                            false,
                        ) {
                            self.emit(&mut log, event);
                        }
                    }
                    queue.push_back(Step::Activate {
                        instance_key,
                        element_id: a.element_id.clone(),
                        scope: 0,
                        via: None,
                    });
                }

                // If the terminations drained the last token and nothing was
                // activated to replace it, the instance is terminated (Zeebe
                // modify semantics) rather than being auto-completed by
                // `complete_finished_instances`.
                let drained = self
                    .state
                    .instances
                    .get(&instance_key)
                    .map(|i| i.active.is_empty())
                    .unwrap_or(true);
                if drained && activate_instructions.is_empty() {
                    self.emit(&mut log, Event::ProcessInstanceTerminated { instance_key });
                }
            }

            Command::DispatchStartInstance {
                process_id,
                start_element_id,
                variables,
                tags,
                business_id,
            } => {
                check_business_id(business_id.as_deref())?;
                // Routed from the deploy partition's StartInstanceDispatched: mint
                // the start-triggered instance here, in this partition's namespace,
                // so start-triggered load spreads across the cluster.
                self.start_instance(
                    &mut log,
                    &mut queue,
                    process_id,
                    start_element_id,
                    variables,
                    tags,
                    business_id,
                    0,
                );
            }

            Command::ActivateAdHocActivities {
                ad_hoc_instance_key,
                activate_elements,
                cancel_remaining,
            } => {
                // External (non-agent-job) ad-hoc activation (#614 gap 3, Zeebe
                // `AdHocSubProcessInstructionActivateProcessor`). Resolve the
                // owning process instance + the container's element id from the
                // element-instance key the caller supplied
                // (`adHocSubProcessInstanceKey`); an unknown/inactive key is
                // rejected NOT_FOUND.
                //
                // An empty activation with no cancel is a no-op the caller never
                // means: it would let `enqueue_adhoc_turn` implicitly complete a
                // parked container (it completes when `already_active + requested
                // == 0`), so an external client could accidentally finish an
                // instance by POSTing `{ "elements": [] }`. Only the agent-job
                // completion seam (#614 gap 4) may end a turn by activating
                // nothing; the external command rejects it as INVALID_ARGUMENT
                // (Zeebe parity). Completion via this command is only expressible
                // through `cancelRemainingInstances`.
                if activate_elements.is_empty() && !cancel_remaining {
                    return Err(EngineError::AdHocNoActivationTargets {
                        ad_hoc_instance_key,
                    });
                }
                let container_key = ad_hoc_instance_key;
                let (instance_key, element_id) = self
                    .state
                    .instances
                    .iter()
                    .find_map(|(ik, inst)| {
                        inst.adhoc_instances
                            .get(&container_key)
                            .map(|a| (*ik, a.element_id.clone()))
                    })
                    .ok_or(EngineError::AdHocSubProcessNotFound {
                        ad_hoc_instance_key,
                    })?;
                let def = self.adhoc_def_of(instance_key, &element_id).ok_or(
                    EngineError::AdHocSubProcessNotFound {
                        ad_hoc_instance_key,
                    },
                )?;
                // Same target validation as the agent-job path (unknown id →
                // NOT_FOUND, atomic), then drive the same activation turn.
                Self::validate_adhoc_activation_targets(&def, instance_key, &activate_elements)?;
                self.enqueue_adhoc_turn(
                    &mut queue,
                    instance_key,
                    container_key,
                    activate_elements,
                    cancel_remaining,
                );
            }
            // AgentInstance lifecycle commands (slice S3). Each processor
            // validates against the stable/8.10 rules, applies any history batch
            // via the attributed pending-history behavior,
            // and emits the CREATED/UPDATED/COMPLETED record event. Validation
            // failures return an `EngineError` and apply nothing.
            Command::CreateAgentInstance {
                element_instance_key,
                job_key,
                job_lease,
                definition,
                limits,
                history,
            } => {
                self.process_create_agent_instance(
                    &mut log,
                    element_instance_key,
                    job_key,
                    job_lease,
                    definition,
                    limits,
                    history,
                )?;
            }
            Command::UpdateAgentInstance {
                agent_instance_key,
                element_instance_key,
                element_id,
                process_instance_key,
                job_key,
                job_lease,
                status,
                metrics,
                tools,
                history,
            } => {
                self.process_update_agent_instance(
                    &mut log,
                    agent_instance_key,
                    element_instance_key,
                    element_id,
                    process_instance_key,
                    job_key,
                    job_lease,
                    status,
                    metrics,
                    tools,
                    history,
                )?;
            }
            Command::CompleteAgentInstance { agent_instance_key } => {
                self.process_complete_agent_instance(&mut log, agent_instance_key)?;
            }
        }

        Ok((log, queue))
    }

    /// Drains the work queue, applying events and enqueuing follow-up steps until
    /// the instance is quiescent. This is the production run-to-completion (RTC)
    /// path: a thin wrapper over [`Engine::run_with`] with a driver that never
    /// pauses. The debugger uses the same `run_with` with a pausing driver, so
    /// the step semantics (`process_step`) are shared, never forked.
    fn run(&mut self, log: &mut Vec<Event>, queue: VecDeque<Step>) {
        let paused = self.run_with(log, queue, 0, &mut RunToCompletion);
        debug_assert!(
            paused.is_none(),
            "RunToCompletion must always drain to quiescence"
        );
    }

    /// The engine's single fixpoint loop, parameterised by a [`StepDriver`] that
    /// is consulted after every processed [`Step`]. When the driver returns
    /// [`Drive::Pause`] the loop returns the captured mid-drain state ([`Paused`])
    /// so a debugger can resume exactly where it left off; when it drains to
    /// quiescence it returns `None`.
    ///
    /// Production code (`run`) passes [`RunToCompletion`], which always continues.
    /// Because this function is generic over the driver, that call site
    /// monomorphizes and the `#[inline]` `Continue` collapses away, so the RTC
    /// instantiation optimizes back down to the old drain loop — same codegen, no
    /// vtable, no per-step overhead. Pausing is strictly additive and only
    /// reachable through the debug entrypoints (which pass their own concrete
    /// drivers) — the RTC contract for real workers is unchanged.
    fn run_with<D: StepDriver>(
        &mut self,
        log: &mut Vec<Event>,
        mut queue: VecDeque<Step>,
        mut cursor: usize,
        driver: &mut D,
    ) -> Option<Paused> {
        loop {
            while let Some(step) = queue.pop_front() {
                let (events, followups) = self.process_step(step, &queue);
                let emitted_from = log.len();
                for event in events {
                    self.emit(log, event);
                }
                for f in followups {
                    queue.push_back(f);
                }
                // Consult the driver with exactly the events this step emitted.
                if let Drive::Pause = driver.after_step(&log[emitted_from..]) {
                    return Some(Paused { queue, cursor });
                }
            }
            // The queue is drained. Any embedded sub-process whose inner scope has
            // emptied completes now and routes along its outgoing flow; that may
            // enqueue more work (and, in turn, drain an enclosing sub-process), so
            // loop until nothing more completes.
            let drain_from = log.len();
            let followups = self.complete_drained_subprocesses(log);
            queue.extend(followups);
            // Re-evaluate any conditional-event subscriptions whose condition may
            // now hold: those opened, or whose referenced variables changed, since
            // the last pass. A satisfied condition fires (advancing a catch token,
            // interrupting an activity, or spawning a non-interrupting token),
            // which enqueues more work — so this runs inside the same fixpoint loop.
            cursor = self.reevaluate_conditionals(log, &mut queue, cursor);
            // Consult the driver on the events the drain sweep itself emitted
            // (a sub-process's own `ElementCompleted`, a conditional/boundary
            // firing) — these do not pass through `process_step`, so without this a
            // breakpoint on such an event would be silently missed. Both sweeps are
            // idempotent (a completed sub-process leaves `instance.active`;
            // `reevaluate_conditionals` advances `cursor`), so a pause here resumes
            // safely: re-entering re-runs them to a no-op. `RunToCompletion` never
            // pauses, so the production path is unchanged.
            if log.len() > drain_from {
                if let Drive::Pause = driver.after_step(&log[drain_from..]) {
                    return Some(Paused { queue, cursor });
                }
            }
            if !queue.is_empty() {
                continue;
            }
            // Token quiescence. Complete any instance whose tokens have all
            // retired — the terminal tail shared with `apply_command_at`, folded
            // into the loop (rather than run post-return in `finish_command`) so a
            // `ProcessCompleted` breakpoint can observe the completion through the
            // driver, exactly like every other event. Idempotent: a completed
            // instance is no longer `Active`, so a resume pass and
            // `finish_command`'s tombstone tail find nothing left to complete, and
            // the production (`RunToCompletion`) log is byte-identical — completion
            // is still emitted once, at the same quiescence point.
            let completed_from = log.len();
            let followups = self.complete_finished_instances(log);
            if log.len() > completed_from {
                if let Drive::Pause = driver.after_step(&log[completed_from..]) {
                    return Some(Paused { queue, cursor });
                }
            }
            // A completion may have released a parked call-activity token; run its
            // completion (and any cascade up the parent chain) before quiescing.
            if !followups.is_empty() {
                queue.extend(followups);
                continue;
            }
            self.debug_assert_join_bookkeeping_coherent(log);
            return None;
        }
    }

    /// Re-evaluates open conditional-event subscriptions against the variable
    /// changes and subscription openings recorded in `log[cursor..]`, firing every
    /// one whose FEEL condition now evaluates `true`. Returns the new cursor (the
    /// log length at entry) so the next pass only considers subsequently appended
    /// events. A no-op (cheap early return) when no conditional subscriptions
    /// exist, which is the common case.
    fn reevaluate_conditionals(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        cursor: usize,
    ) -> usize {
        let scan_end = log.len();
        if self.state.conditional_subscriptions.is_empty() {
            return scan_end;
        }
        // Which variables changed, per instance, and which conditional
        // subscriptions were opened, since the last pass.
        let mut changed: HashMap<Key, std::collections::HashSet<String>> = HashMap::new();
        let mut opened: std::collections::HashSet<Key> = std::collections::HashSet::new();
        for event in &log[cursor..scan_end] {
            match event {
                Event::VariablesUpdated {
                    instance_key,
                    variables,
                } => {
                    let entry = changed.entry(*instance_key).or_default();
                    for name in variables.keys() {
                        entry.insert(name.clone());
                    }
                }
                Event::ProcessInstanceCreated {
                    instance_key,
                    variables,
                    ..
                } => {
                    // Initial variables count as a change (a conditional event may
                    // already hold at activation).
                    let entry = changed.entry(*instance_key).or_default();
                    for name in variables.keys() {
                        entry.insert(name.clone());
                    }
                }
                Event::ConditionalSubscriptionCreated {
                    subscription_key, ..
                } => {
                    opened.insert(*subscription_key);
                }
                _ => {}
            }
        }
        if changed.is_empty() && opened.is_empty() {
            return scan_end;
        }
        // Collect the subscriptions to (re-)evaluate, deterministically by key: a
        // newly opened one, or one whose referenced variables changed.
        let mut candidates: Vec<Key> = self
            .state
            .conditional_subscriptions
            .values()
            .filter(|s| {
                s.state == state::MessageSubscriptionState::Open
                    && (opened.contains(&s.key)
                        || changed
                            .get(&s.instance_key)
                            .is_some_and(|vars| s.referenced_vars.iter().any(|v| vars.contains(v))))
            })
            .map(|s| s.key)
            .collect();
        candidates.sort_unstable();

        for key in candidates {
            // A prior fire in this batch may have interrupted the activity and
            // cancelled this subscription, so re-check it is still open.
            let Some(sub) = self.state.conditional_subscriptions.get(&key) else {
                continue;
            };
            if sub.state != state::MessageSubscriptionState::Open {
                continue;
            }
            let instance_key = sub.instance_key;
            let element_instance_key = sub.element_instance_key;
            let element_id = sub.element_id.clone();
            let condition = sub.condition.clone();
            let kind = sub.kind.clone();
            let vars = self.variables(instance_key);
            if !matches!(crate::feel::eval_bool(&condition, &vars), Ok(true)) {
                continue;
            }
            self.emit(
                log,
                Event::ConditionalTriggered {
                    subscription_key: key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                },
            );
            match kind {
                // Intermediate catch: releasing it resumes the token along its own
                // outgoing flow.
                state::MessageSubscriptionKind::IntermediateCatch => {
                    queue.push_back(Step::Complete {
                        instance_key,
                        element_instance_key,
                        element_id,
                    });
                }
                // Interrupting boundary: interrupt the attached activity, then run
                // the boundary event's outgoing flow.
                state::MessageSubscriptionKind::InterruptingBoundary {
                    boundary_element_id,
                } => {
                    let scope = self.scope_of(instance_key, element_instance_key);
                    self.interrupt_activity_via_boundary(
                        log,
                        instance_key,
                        element_instance_key,
                        &element_id,
                    );
                    queue.push_back(Step::Activate {
                        instance_key,
                        element_id: boundary_element_id,
                        scope,
                        via: None,
                    });
                }
                // Non-interrupting boundary: leave the activity running and spawn a
                // parallel token along the boundary's outgoing flow. The
                // subscription stays open (its applier does not settle it), so a
                // later change to a referenced variable can fire it again.
                state::MessageSubscriptionKind::NonInterruptingBoundary {
                    boundary_element_id,
                } => {
                    queue.push_back(Step::Activate {
                        instance_key,
                        element_id: boundary_element_id,
                        scope: self.scope_of(instance_key, element_instance_key),
                        via: None,
                    });
                }
            }
        }
        scan_end
    }

    /// Records a correlation against a subscription whose **instance lives on this
    /// partition** and advances its parked token: emits [`Event::MessageCorrelated`]
    /// (settling the subscription unless it is a non-interrupting boundary), merges
    /// the message's `variables`, then enqueues the catch/boundary outcome. Shared
    /// by the inline local correlation in `CorrelateMessage` and the
    /// `CorrelateMessageSubscription` continuation routed back from a message
    /// partition, so both produce identical token-advance behaviour.
    #[allow(clippy::too_many_arguments)]
    fn advance_correlated_token(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        subscription_key: Key,
        message_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        kind: state::MessageSubscriptionKind,
        variables: &HashMap<String, Value>,
    ) {
        self.emit(
            log,
            Event::MessageCorrelated {
                subscription_key,
                message_key,
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
        );
        // The message's variables (if any) are merged into the correlated
        // instance before its token advances, propagating from the catching
        // element's enclosing (flow) scope upward (nearest defining ancestor,
        // else root). Root-only instances collapse to the flat `VariablesUpdated`.
        if !variables.is_empty() {
            let flow_scope = self.scope_of(instance_key, element_instance_key);
            for event in self.propagated_updates(instance_key, flow_scope, variables.clone(), false)
            {
                self.emit(log, event);
            }
        }

        match kind {
            // Catch event: completing it resumes the token along its own outgoing
            // flow.
            state::MessageSubscriptionKind::IntermediateCatch => {
                queue.push_back(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id,
                });
            }
            // Boundary subscription: interrupt the attached activity (a service
            // task or sub-process), then run the boundary event's outgoing flow.
            // `element_instance_key`/`element_id` are the activity here.
            state::MessageSubscriptionKind::InterruptingBoundary {
                boundary_element_id,
            } => {
                let scope = self.scope_of(instance_key, element_instance_key);
                self.interrupt_activity_via_boundary(
                    log,
                    instance_key,
                    element_instance_key,
                    &element_id,
                );
                queue.push_back(Step::Activate {
                    instance_key,
                    element_id: boundary_element_id,
                    scope,
                    via: None,
                });
            }
            // Non-interrupting boundary subscription: leave the activity (and its
            // job) running and spawn a parallel token along the boundary event's
            // outgoing flow, in the activity's scope. The subscription stays open
            // (its applier does not settle it), so the next matching message spawns
            // another token.
            state::MessageSubscriptionKind::NonInterruptingBoundary {
                boundary_element_id,
            } => {
                queue.push_back(Step::Activate {
                    instance_key,
                    element_id: boundary_element_id,
                    scope: self.scope_of(instance_key, element_instance_key),
                    via: None,
                });
            }
        }
    }

    /// Advances a token whose signal subscription just correlated, mirroring
    /// [`Self::advance_correlated_token`] but for signals (name-only, always
    /// local). Emits [`Event::SignalCorrelated`] + variable merge, then queues
    /// the catch completion or boundary interrupt/spawn.
    #[allow(clippy::too_many_arguments)]
    fn advance_signal_correlated_token(
        &mut self,
        log: &mut Vec<Event>,
        queue: &mut VecDeque<Step>,
        subscription_key: Key,
        signal_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: ElementId,
        kind: state::MessageSubscriptionKind,
        variables: &HashMap<String, Value>,
    ) {
        self.emit(
            log,
            Event::SignalCorrelated {
                subscription_key,
                signal_key,
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
        );
        if !variables.is_empty() {
            // Signal payload propagates from the catching element's enclosing
            // (flow) scope upward, defaulting to root.
            let flow_scope = self.scope_of(instance_key, element_instance_key);
            for event in self.propagated_updates(instance_key, flow_scope, variables.clone(), false)
            {
                self.emit(log, event);
            }
        }

        match kind {
            state::MessageSubscriptionKind::IntermediateCatch => {
                queue.push_back(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id,
                });
            }
            state::MessageSubscriptionKind::InterruptingBoundary {
                boundary_element_id,
            } => {
                let scope = self.scope_of(instance_key, element_instance_key);
                self.interrupt_activity_via_boundary(
                    log,
                    instance_key,
                    element_instance_key,
                    &element_id,
                );
                queue.push_back(Step::Activate {
                    instance_key,
                    element_id: boundary_element_id,
                    scope,
                    via: None,
                });
            }
            state::MessageSubscriptionKind::NonInterruptingBoundary {
                boundary_element_id,
            } => {
                queue.push_back(Step::Activate {
                    instance_key,
                    element_id: boundary_element_id,
                    scope: self.scope_of(instance_key, element_instance_key),
                    via: None,
                });
            }
        }
    }

    /// Completes every active sub-process element instance whose inner token
    /// scope has drained (no remaining child element instances), emitting its
    /// completion events and returning the follow-up activations for its outgoing
    /// flows. Deterministic in `(instance_key, element_instance_key)` order.
    fn complete_drained_subprocesses(&mut self, log: &mut Vec<Event>) -> Vec<Step> {
        // Only an instance whose tokens moved this command can have a *newly*
        // drained sub-process scope (draining requires consuming a child token,
        // which emits an event), so restrict the sweep to the instances touched in
        // the log — exactly as `complete_finished_instances` does. Scanning every
        // active instance instead made this O(total active backlog) per command:
        // under a large undrained backlog that scan dominated the engine thread
        // (per-command cost grew linearly with the backlog).
        let touched: HashSet<Key> = log.iter().filter_map(|e| e.instance_key()).collect();
        let mut drained: Vec<(Key, Key, ElementId)> = Vec::new();
        for instance_key in &touched {
            let Some(instance) = self.state.instances.get(instance_key) else {
                continue;
            };
            if instance.state != ProcessInstanceState::Active {
                continue;
            }
            let Some(process) = self.state.definition_for(instance) else {
                continue;
            };
            for (eik, element_id) in &instance.active {
                let is_subprocess = matches!(
                    process.definition.element(element_id).map(|e| &e.kind),
                    Some(ElementKind::SubProcess { .. })
                );
                if !is_subprocess {
                    continue;
                }
                let has_child = instance.scopes.values().any(|parent| parent == eik);
                // A sub-process parked on an open incident (an input-mapping failure
                // raised during activation, before its start event ran) has no inner
                // token either, but it is NOT drained: it never entered its body.
                // Completing it here would advance the token past the sub-process
                // with its work silently skipped (and strand the incident on a
                // completed element). Leave it parked; resolving the incident
                // re-drives the activation (`RetryActivation`, #946).
                let parked_on_incident = instance.incidents.iter().any(|k| {
                    self.state
                        .incidents
                        .get(k)
                        .is_some_and(|i| i.element_instance_key == *eik)
                });
                if !has_child && !parked_on_incident && self.active_job_on(*eik).is_none() {
                    // A sub-process resting in COMPLETING while its `end`
                    // execution-listener chain runs (ADR 0037) has also drained its
                    // children, but carries a parked listener job on its own
                    // instance — the only kind of job a sub-process instance can
                    // host, since it creates none of its own. Skip it so the sweep
                    // does not re-fire its completion; `finalize_subprocess` emits
                    // the deferred `ElementCompleted` when the chain drains.
                    drained.push((instance.key, *eik, element_id.clone()));
                }
            }
        }
        drained.sort();

        let mut followups = Vec::new();
        for (instance_key, eik, element_id) in drained {
            // The sub-process completes in its own (parent) scope, captured before
            // its scope entry is cleared by `ElementCompleted`.
            let scope = self.scope_of(instance_key, eik);

            // A drained sub-process whose enclosing scope is a multi-instance body
            // (with a matching element id) is a MULTI-INSTANCE CHILD: its
            // completion must feed the loop's output collection + join / next
            // child rather than take the activity's outgoing flow. `complete`
            // detects this and delegates to `complete_mi_child`, so route it
            // there via `Step::Complete` (which emits its `ElementCompleted` and
            // tears down the child scope). Cancel any boundary events armed on the
            // child first — the MI completion path does not follow the normal
            // boundary-cleanup branch below. Its `output_element` (the MI output),
            // not the sub-process's `zeebe:ioMapping` outputs, is what aggregates.
            let is_mi_child = self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.multi_instances.get(&scope))
                .map(|mi| mi.element_id == element_id)
                .unwrap_or(false);
            if is_mi_child {
                for event in self.cancel_boundary_timers_on(eik) {
                    self.emit(log, event);
                }
                for event in self.cancel_boundary_message_subscriptions_on(eik) {
                    self.emit(log, event);
                }
                for event in self.cancel_boundary_signal_subscriptions_on(eik) {
                    self.emit(log, event);
                }
                for event in self.cancel_boundary_conditional_subscriptions_on(eik) {
                    self.emit(log, event);
                }
                followups.push(Step::Complete {
                    instance_key,
                    element_instance_key: eik,
                    element_id,
                });
                continue;
            }

            // A drained sub-process that is an embedded-subProcess ad-hoc TOOL
            // (#872): it hangs off an `#innerInstance` whose own scope is the
            // ad-hoc container that still lists it active. Its completion must feed
            // the container's `outputCollection` + activate-element loop and tear
            // down the inner instance — not take an outgoing flow (it has none). It
            // routes there via `Step::Complete`, which `complete` dispatches to
            // `complete_adhoc_tool` by the same scope-chain check the leaf-tool
            // completion path uses. Disarm any boundary events on the tool first,
            // mirroring the multi-instance-child branch above (the ad-hoc
            // completion path does not run the normal boundary-cleanup branch).
            let is_adhoc_tool = {
                let container = self.scope_of(instance_key, scope);
                container != 0
                    && self
                        .state
                        .instances
                        .get(&instance_key)
                        .and_then(|i| i.adhoc_instances.get(&container))
                        .map(|a| a.active.contains(&eik))
                        .unwrap_or(false)
            };
            if is_adhoc_tool {
                for event in self.cancel_boundary_timers_on(eik) {
                    self.emit(log, event);
                }
                for event in self.cancel_boundary_message_subscriptions_on(eik) {
                    self.emit(log, event);
                }
                for event in self.cancel_boundary_signal_subscriptions_on(eik) {
                    self.emit(log, event);
                }
                for event in self.cancel_boundary_conditional_subscriptions_on(eik) {
                    self.emit(log, event);
                }
                followups.push(Step::Complete {
                    instance_key,
                    element_instance_key: eik,
                    element_id,
                });
                continue;
            }
            // the enclosing (parent) scope. Capture the values now; emit them as
            // scoped writes after `ElementCompleted` has dropped the local scope.
            let outputs = self.io_outputs(instance_key, &element_id);
            let output_updates = if outputs.is_empty() {
                HashMap::new()
            } else {
                let visible = self.variables_for_element(instance_key, eik);
                match self.eval_io_mappings_in(&visible, &outputs) {
                    Ok(updates) => updates,
                    Err(failure) => {
                        // An output mapping that fails to evaluate halts the
                        // sub-process in COMPLETING with an incident rather than
                        // completing it with the target unset (#939). Skip this
                        // sub-process for the rest of the sweep; it stays active
                        // parked on the incident (re-driven by `Complete` on
                        // resolution).
                        let event = self.io_mapping_incident(
                            instance_key,
                            eik,
                            element_id.clone(),
                            failure,
                            state::IoMappingRedrive::Completion,
                        );
                        self.emit(log, event);
                        continue;
                    }
                }
            };
            self.emit(
                log,
                Event::ElementCompleting {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                },
            );

            // End-listener gate (ADR 0037): the sub-process rests in COMPLETING
            // while its `end` chain runs. Its boundary events disarm and its
            // output mappings project at completing-time (Zeebe: before the
            // listeners); `finalize_subprocess` emits the deferred
            // `ElementCompleted` + outgoing flows once the chain drains. The
            // parked listener job keeps the sub-process instance active, and the
            // sweep guard above skips it so it is not re-detected as drained.
            // Only build the (cloned) listener variable view when the sub-process
            // actually declares `end` listeners — the listener-free path stays
            // allocation-free, matching the pre-listener engine operationally.
            let has_end_listeners = !self
                .listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::ListenerEventType::End,
                )
                .is_empty();
            if has_end_listeners {
                let mut listener_vars = (*self.variables_for_element(instance_key, eik)).clone();
                // Zeebe runs `end` listeners after output mappings, so the first
                // listener resolves its own FEEL (job type / retries) against the
                // mapped values — the same post-output view every subsequent
                // listener sees (they re-read the scope after the propagation
                // below is applied).
                listener_vars.extend(output_updates.clone());
                if let Some(job) = self.begin_end_listener_chain(
                    instance_key,
                    eik,
                    &element_id,
                    scope,
                    &listener_vars,
                ) {
                    for event in self.cancel_boundary_timers_on(eik) {
                        self.emit(log, event);
                    }
                    for event in self.cancel_boundary_message_subscriptions_on(eik) {
                        self.emit(log, event);
                    }
                    for event in self.cancel_boundary_signal_subscriptions_on(eik) {
                        self.emit(log, event);
                    }
                    for event in self.cancel_boundary_conditional_subscriptions_on(eik) {
                        self.emit(log, event);
                    }
                    if !output_updates.is_empty() {
                        for event in
                            self.propagated_updates(instance_key, scope, output_updates, false)
                        {
                            self.emit(log, event);
                        }
                    }
                    self.emit(log, job);
                    continue;
                }
            }

            self.emit(
                log,
                Event::ElementCompleted {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                },
            );
            // Completing normally disarms any boundary timers/subscriptions on it.
            for event in self.cancel_boundary_timers_on(eik) {
                self.emit(log, event);
            }
            for event in self.cancel_boundary_message_subscriptions_on(eik) {
                self.emit(log, event);
            }
            for event in self.cancel_boundary_signal_subscriptions_on(eik) {
                self.emit(log, event);
            }
            for event in self.cancel_boundary_conditional_subscriptions_on(eik) {
                self.emit(log, event);
            }
            if !output_updates.is_empty() {
                for event in self.propagated_updates(instance_key, scope, output_updates, false) {
                    self.emit(log, event);
                }
            }
            for (taken, step) in self.take_all_flows(instance_key, &element_id, scope) {
                for event in taken {
                    self.emit(log, event);
                }
                followups.push(step);
            }
        }
        followups
    }

    /// Whether a queued step would (re)create work on an instance that has
    /// already `Terminated`, or under a sub-process scope / element instance /
    /// multi-instance body / ad-hoc container that teardown has already removed —
    /// in which case `process_step` drops it (see the terminal-scope guard
    /// there). **Every** step variant that names an instance is subject to the
    /// instance-terminal check (a `Terminated` instance can hold no more live
    /// work of any kind); element-scoped, body/container-scoped and listener/
    /// retry steps additionally verify their target still exists. The lone
    /// exception is a cross-instance `CompleteCallActivity`, which targets a
    /// still-live *parent* and whose own handler already tolerates a
    /// vanished call-activity token — so only its parent-terminal check applies.
    fn step_targets_dead_scope(&self, step: &Step) -> bool {
        // (instance_key, activation target scope, element-instance target,
        //  multi-instance body / ad-hoc container owner)
        let (instance_key, scope, eik, owner) = match step {
            Step::Activate {
                instance_key,
                scope,
                ..
            } => (*instance_key, Some(*scope), None, None),
            Step::Complete {
                instance_key,
                element_instance_key,
                ..
            }
            | Step::CompleteAdHocCallActivityTool {
                instance_key,
                element_instance_key,
                ..
            }
            | Step::CreateJob {
                instance_key,
                element_instance_key,
                ..
            }
            | Step::ReopenCatch {
                instance_key,
                element_instance_key,
                ..
            }
            | Step::RetryActivation {
                instance_key,
                element_instance_key,
                ..
            }
            | Step::AdvanceListener {
                instance_key,
                element_instance_key,
                ..
            }
            | Step::RetryCallActivitySpawn {
                instance_key,
                element_instance_key,
                ..
            } => (*instance_key, None, Some(*element_instance_key), None),
            Step::ActivateMiChild {
                instance_key,
                body_key,
                ..
            }
            | Step::CompleteMiBody {
                instance_key,
                body_key,
            } => (*instance_key, None, None, Some(*body_key)),
            Step::ActivateAdHocTool {
                instance_key,
                container_key,
                ..
            }
            | Step::CompleteAdHoc {
                instance_key,
                container_key,
                ..
            } => (*instance_key, None, None, Some(*container_key)),
            // A multi-instance **child** retry re-drives the *already-activated*
            // child (its activation events are not re-emitted, #946), so both its
            // child token and its owning body must still exist: a scoped terminate
            // that tore the loop down between queueing and draining this retry must
            // drop it, exactly like the non-retry MI steps above. Thread the child
            // (element-instance) and body (owner) keys so the shared checks below
            // apply — reusing the same "did teardown remove it?" logic.
            Step::RetryMiChildActivation {
                instance_key,
                body_key,
                child_key,
                ..
            } => (*instance_key, None, Some(*child_key), Some(*body_key)),
            // A multi-instance **body** retry re-drives the *already-activated*
            // body token (its activation/scope events are not re-emitted), so the
            // body element instance must still be active. Its `multi_instances`
            // record may legitimately not exist yet — the retry re-runs the very
            // fan-out that creates it — so only the element-instance check applies,
            // not the owner check.
            Step::RetryMiBodyActivation {
                instance_key,
                body_key,
                ..
            } => (*instance_key, None, Some(*body_key), None),
            // A cross-instance `CompleteCallActivity` targets the still-live
            // parent, and its handler already tolerates a vanished call-activity
            // token — only the instance-terminal check applies.
            Step::CompleteCallActivity { instance_key, .. } => (*instance_key, None, None, None),
            // Task-keyed: resolve the owning instance through the user task, and
            // reject the step unless the task is still `Created`. `UserTaskCanceled`
            // deliberately *retains* the record (state → `Canceled`, `pending`
            // cleared), so a bare existence check would let an `AdvanceTaskListener`
            // queued before a terminate teardown mint another listener job against
            // an already-cancelled (or completed) task.
            Step::AdvanceTaskListener { user_task_key, .. } => {
                match self.state.user_tasks.get(user_task_key) {
                    Some(t) if t.state == crate::state::UserTaskState::Created => {
                        (t.instance_key, None, None, None)
                    }
                    // Gone, or no longer `Created` (canceled/completed): drop it.
                    _ => return true,
                }
            }
        };
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return true; // the whole instance is gone
        };
        if instance.state == ProcessInstanceState::Terminated {
            return true;
        }
        // A non-root activation scope that no longer exists: not an active
        // element instance, nor a multi-instance body, nor an ad-hoc container —
        // or one whose teardown was *decided* this drain but whose sub-process
        // token is still active pending the post-drain completion sweep (a scoped
        // terminate / interrupting boundary leaves it live until then, so a still-
        // queued sibling activation must be rejected here rather than recreate a
        // token inside the dead scope and wedge the drain).
        if let Some(scope) = scope {
            if scope != 0
                && (self.torn_down_scopes.contains(&scope)
                    || (!instance.active.contains_key(&scope)
                        && !instance.multi_instances.contains_key(&scope)
                        && !instance.adhoc_instances.contains_key(&scope)))
            {
                return true;
            }
        }
        // A completion/re-drive target whose element instance teardown removed.
        if let Some(eik) = eik {
            if !instance.active.contains_key(&eik) {
                return true;
            }
        }
        // A multi-instance body / ad-hoc container owner teardown removed: its
        // runtime record is gone, so a queued child activation or body/container
        // completion would mint work into a dead loop.
        if let Some(owner) = owner {
            if !instance.multi_instances.contains_key(&owner)
                && !instance.adhoc_instances.contains_key(&owner)
            {
                return true;
            }
        }
        false
    }

    /// A flow into a join is counted when it is taken ([`Engine::take_flow`]),
    /// so a join activation the terminal-scope guard drops has already been
    /// counted. If no join instance is open to hold it, that arrival belongs to
    /// the dead scope and would otherwise survive as a phantom token a
    /// re-entered scope fires against: reset the join, as Zeebe's count dies
    /// with its flow-scope instance. An open join instance in the dead scope is
    /// already reset by [`scope_teardown_events`](Self::scope_teardown_events).
    fn revoke_dropped_join_arrival(&self, step: &Step) -> Vec<Event> {
        let Step::Activate {
            instance_key,
            element_id,
            via: Some(_),
            ..
        } = step
        else {
            return Vec::new();
        };
        let holds_tokens = self.state.instances.get(instance_key).is_some_and(|i| {
            i.join_flow_arrivals.contains_key(element_id) || i.join_counts.contains_key(element_id)
        });
        if holds_tokens
            && self.join_kind(*instance_key, element_id).is_some()
            && self.join_eik(*instance_key, element_id).is_none()
        {
            vec![Event::ParallelJoinReset {
                instance_key: *instance_key,
                element_id: element_id.clone(),
            }]
        } else {
            Vec::new()
        }
    }

    /// Debug-build guard for the join-bookkeeping defect class: once a drain
    /// quiesces nothing is in transit, so every join still holding tokens must
    /// have an open join instance holding them (the TLA+ `TokenFlow`
    /// `JoinBookkeepingCoherent` invariant with no activation queued). A token
    /// with no open instance is a phantom a later arrival would fire against.
    fn debug_assert_join_bookkeeping_coherent(&self, log: &[Event]) {
        if !cfg!(debug_assertions) {
            return;
        }
        let touched: HashSet<Key> = log.iter().filter_map(|e| e.instance_key()).collect();
        for key in touched {
            let Some(instance) = self.state.instances.get(&key) else {
                continue;
            };
            if instance.state != ProcessInstanceState::Active {
                continue;
            }
            for join in instance
                .join_flow_arrivals
                .keys()
                .chain(instance.join_counts.keys())
            {
                debug_assert!(
                    instance.join_instances.contains_key(join),
                    "instance {key}: join `{join}` holds tokens but no open join instance"
                );
            }
        }
    }

    /// The processor: decides the events and follow-up work for one lifecycle
    /// step. Reads state, mints keys, but never mutates [`State`]. `pending` is
    /// the rest of the step queue: the flows taken but not yet activated, which
    /// an inclusive join's readiness guard counts as live paths.
    fn process_step(&mut self, step: Step, pending: &VecDeque<Step>) -> (Vec<Event>, Vec<Step>) {
        // Terminal-scope guard (the single dispatch choke point). A queued step
        // can sit behind the very teardown that invalidates it: a top-level
        // terminate end marks its instance `Terminated`, and a sub-process-scoped
        // terminate (or an interrupting boundary) removes the scope its inner
        // tokens live in. `activate` / `complete` / `create_job_for` carry no
        // terminal check of their own, so without this a sibling step still in
        // the queue could recreate an element, job or token on a dead instance —
        // or under a removed sub-process scope — after teardown (and a resolved
        // incident inside a torn-down scope could re-drive a vanished token). Drop
        // such a step here. `Terminating` (a deferred cancel still draining its
        // own listeners) is deliberately NOT guarded — it must run its remaining
        // steps to finish.
        if self.step_targets_dead_scope(&step) {
            return (self.revoke_dropped_join_arrival(&step), Vec::new());
        }
        match step {
            Step::Activate {
                instance_key,
                element_id,
                scope,
                via,
            } => self.activate(instance_key, element_id, scope, via, pending),
            Step::Complete {
                instance_key,
                element_instance_key,
                element_id,
            } => self.complete(instance_key, element_instance_key, element_id),
            Step::CompleteAdHocCallActivityTool {
                instance_key,
                element_instance_key,
                element_id,
                container_key,
                inner_key,
                output_updates,
            } => self.complete_adhoc_tool(
                instance_key,
                element_instance_key,
                element_id,
                container_key,
                inner_key,
                Some(output_updates),
            ),
            Step::CreateJob {
                instance_key,
                element_instance_key,
                element_id,
            } => self.create_job_for(instance_key, element_instance_key, element_id),
            Step::ReopenCatch {
                instance_key,
                element_instance_key,
                element_id,
            } => {
                // The catch element instance is already ACTIVATED (its token is
                // parked on the resolved incident); reconstruct its enclosing
                // scope (absent from `scopes` ⇒ root) and re-derive only the
                // activation body to re-open the subscription.
                let scope = self.scope_of(instance_key, element_instance_key);
                let vars = self.variables_for_element(instance_key, element_instance_key);
                self.run_activation_body(
                    instance_key,
                    element_id,
                    element_instance_key,
                    scope,
                    vars,
                )
            }
            Step::RetryActivation {
                instance_key,
                element_instance_key,
                element_id,
            } => {
                // The element instance is already ACTIVATED (parked on the
                // resolved input-mapping incident); reconstruct its enclosing
                // scope and re-run the activation body so the now-fixed input
                // mappings are re-applied before the element's behaviour runs.
                let scope = self.scope_of(instance_key, element_instance_key);
                self.activate_body(instance_key, element_id, element_instance_key, scope)
            }
            Step::ActivateMiChild {
                instance_key,
                element_id,
                body_key,
                index,
            } => self.activate_mi_child(instance_key, element_id, body_key, index),
            Step::CompleteMiBody {
                instance_key,
                body_key,
            } => self.complete_multi_instance_body(instance_key, body_key),
            Step::ActivateAdHocTool {
                instance_key,
                container_key,
                element_id,
                variables,
            } => self.activate_adhoc_tool(instance_key, container_key, element_id, variables),
            Step::CompleteAdHoc {
                instance_key,
                container_key,
                cancel,
            } => self.complete_adhoc_container(instance_key, container_key, cancel),
            Step::AdvanceListener {
                instance_key,
                element_instance_key,
                element_id,
                event_type,
                index,
                scope,
            } => self.advance_listener(
                instance_key,
                element_instance_key,
                element_id,
                event_type,
                index,
                scope,
            ),
            Step::AdvanceTaskListener {
                user_task_key,
                event_type,
                index,
            } => self.advance_task_listener(user_task_key, event_type, index),
            Step::CompleteCallActivity {
                instance_key,
                element_instance_key,
                element_id,
                child_variables,
            } => self.complete_call_activity(
                instance_key,
                element_instance_key,
                element_id,
                child_variables,
            ),
            Step::RetryMiChildActivation {
                instance_key,
                element_id,
                body_key,
                child_key,
                index,
            } => {
                self.retry_mi_child_activation(instance_key, element_id, body_key, child_key, index)
            }
            Step::RetryMiBodyActivation {
                instance_key,
                element_id,
                body_key,
                scope,
            } => {
                // Re-derive the activity's multi-instance model from the (immutable)
                // process definition, mirroring the first activation.
                match self.multi_instance_of(instance_key, &element_id) {
                    Some(mi) => {
                        self.run_mi_body_activation(instance_key, element_id, body_key, scope, mi)
                    }
                    None => (Vec::new(), Vec::new()),
                }
            }
            Step::RetryCallActivitySpawn {
                instance_key,
                element_instance_key,
                element_id,
                preserved_seed,
            } => self.retry_call_activity_spawn(
                instance_key,
                element_instance_key,
                element_id,
                preserved_seed,
            ),
        }
    }

    fn activate(
        &mut self,
        instance_key: Key,
        element_id: String,
        scope: Key,
        via: Option<IncomingFlow>,
        pending: &VecDeque<Step>,
    ) -> (Vec<Event>, Vec<Step>) {
        // A parallel or inclusive gateway with more than one incoming flow is a
        // join: its activation is guarded instead of running per arrival.
        if let Some(join) = self.join_kind(instance_key, &element_id) {
            return self.activate_join(instance_key, element_id, scope, via, join, pending);
        }
        let kind = self.element_kind(instance_key, &element_id);

        // A multi-instance activity: unless we are already inside its body (i.e.
        // this is one of its children, running in the body scope), this
        // activation opens the multi-instance body — it evaluates the input
        // collection and fans out one child per item instead of instantiating a
        // single activity.
        let is_mi_child = scope != 0
            && self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.multi_instances.get(&scope))
                .map(|mi| mi.element_id == element_id)
                .unwrap_or(false);
        if !is_mi_child {
            if let Some(mi) = self.multi_instance_of(instance_key, &element_id) {
                return self.activate_multi_instance_body(instance_key, element_id, scope, mi);
            }
        }

        let element_instance_key = self.mint_key();
        let mut events = vec![
            Event::ElementActivating {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            },
            Event::ElementActivated {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
                scope,
            },
        ];
        let mut followups = Vec::new();

        // A sub-process is itself a variable scope: it always registers its scope
        // (so writes inside it can resolve/propagate correctly) and its inputs are
        // local to that scope, exactly like a leaf activity. An ad-hoc sub-process
        // container (a job-bearing ServiceTask that appears in the definition's
        // ad-hoc catalog) is likewise a variable scope — its activated tool
        // children run inside it and its `outputCollection` accumulates there — so
        // it always registers a scope (ADR 0023 seam 2). Registered here, on first
        // activation, BEFORE the input mappings run; a resolved input-mapping
        // incident re-runs only `activate_body`, which sees the scope already
        // registered and does not re-create it.
        let is_sub_process = matches!(kind, Some(ElementKind::SubProcess { .. }));
        let adhoc_def = self.adhoc_def_of(instance_key, &element_id);
        let is_adhoc_container = adhoc_def.is_some();
        if is_sub_process || is_adhoc_container {
            events.push(Event::VariableScopeCreated {
                instance_key,
                scope_key: element_instance_key,
                parent_scope_key: scope,
            });
        }

        let (body_events, body_followups) =
            self.activate_body(instance_key, element_id, element_instance_key, scope);
        events.extend(body_events);
        followups.extend(body_followups);
        (events, followups)
    }

    /// The element's activation body: everything between `ElementActivated` and
    /// the element's own behaviour. Seeds any ad-hoc `outputCollection`, applies
    /// the input `zeebe:ioMapping`s LOCAL to the element's own scope, runs the
    /// `start` execution-listener gate, then (listener-free) runs
    /// [`run_activation_body`]. Split out of [`activate`] so a resolved
    /// input-mapping incident can re-drive it ([`Step::RetryActivation`]) to
    /// re-apply the now-fixed mappings and re-enact the behaviour without
    /// re-emitting `ElementActivating`/`ElementActivated` or re-registering the
    /// scope.
    ///
    /// An input mapping whose source expression fails to evaluate raises an
    /// `IO_MAPPING_ERROR` incident and halts the element (returning only the
    /// incident event, no behaviour) rather than silently proceeding with the
    /// target variable unset — matching Zeebe (#939).
    fn activate_body(
        &mut self,
        instance_key: Key,
        element_id: String,
        element_instance_key: Key,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        // Input mappings (zeebe:input): evaluate against the variables visible to
        // the activating element and create the mapped values LOCAL to the
        // element's own scope (Zeebe semantics) — visible to the element's job or
        // inner flow, not propagated to the parent, and dropped when the element
        // completes.
        let kind = self.element_kind(instance_key, &element_id);
        let is_sub_process = matches!(kind, Some(ElementKind::SubProcess { .. }));
        let is_call_activity = matches!(kind, Some(ElementKind::CallActivity { .. }));
        let adhoc_def = self.adhoc_def_of(instance_key, &element_id);
        let is_adhoc_container = adhoc_def.is_some();
        // The element's own scope is registered by `activate` for a
        // sub-process/ad-hoc container (before this body runs); a leaf activity
        // registers it lazily below, only if its inputs produce values.
        let scope_registered = is_sub_process || is_adhoc_container;
        // A call activity's input mappings seed only its isolated child process
        // scope (applied in `spawn_call_activity_child`) — never the call
        // activity's own element scope — so they are NOT projected here. Routing
        // them through the spawn also means a failure re-drives the spawn
        // (`CallActivitySpawn`) rather than re-running this whole activation body
        // (which would re-arm the already-armed boundary events).
        let inputs = if is_call_activity {
            Vec::new()
        } else {
            self.io_inputs(instance_key, &element_id)
        };

        // The scoped variable view the activating element evaluates against: both
        // its input-mapping *source* expressions and its own FEEL attributes (job
        // type, retries, priority, timer/message/signal name, correlation key,
        // user-task fields). It is the element's enclosing flow scope — a
        // root-scope element gets the shared root Arc (a cheap refcount bump), so
        // evaluation is byte-identical to the flat engine; an element inside a
        // sub-process or multi-instance body additionally sees its enclosing
        // scope's locals, so an input mapping that reads an enclosing-scope-local
        // variable resolves correctly (Zeebe parity). A sub-process's own inputs
        // are evaluated here against its *parent* scope and then applied local to
        // the new sub-process scope.
        let element_vars = self.variables_for_element(instance_key, scope);

        let mut events: Vec<Event> = Vec::new();
        let followups: Vec<Step> = Vec::new();

        // Seed the ad-hoc container's `outputCollection` to an empty array as a
        // local variable the moment it activates (Zeebe
        // `AdHocSubProcessProcessor.onActivate`), BEFORE the container's own input
        // mappings run — so the agent can read the growing collection mid-run, and
        // a mis-mapped non-array target is observable to the append-time type
        // guard. This variable is the single source of truth for the accumulated
        // tool outputs; `complete_adhoc_tool` appends to it and
        // `complete_adhoc_container` propagates it outward.
        if let Some(name) = adhoc_def.as_ref().and_then(|d| d.output_collection.clone()) {
            events.push(Event::ScopedVariablesUpdated {
                instance_key,
                scope_key: element_instance_key,
                variables: HashMap::from([(name, Value::List(Vec::new()))]),
            });
        }
        // A source expression that fails to evaluate raises an `IO_MAPPING_ERROR`
        // incident and halts the element (no scope write, no behaviour) — the
        // resolution re-drives this body (#939).
        let input_updates = if inputs.is_empty() {
            HashMap::new()
        } else {
            match self.eval_io_mappings_in(&element_vars, &inputs) {
                Ok(updates) => updates,
                Err(failure) => {
                    return (
                        vec![self.io_mapping_incident(
                            instance_key,
                            element_instance_key,
                            element_id,
                            failure,
                            state::IoMappingRedrive::Activation,
                        )],
                        Vec::new(),
                    );
                }
            }
        };
        if !input_updates.is_empty() {
            if !scope_registered {
                events.push(Event::VariableScopeCreated {
                    instance_key,
                    scope_key: element_instance_key,
                    parent_scope_key: scope,
                });
            }
            events.push(Event::ScopedVariablesUpdated {
                instance_key,
                scope_key: element_instance_key,
                variables: input_updates.clone(),
            });
        }

        // Scope writes above have not been applied yet. Both listeners and the
        // activation body must evaluate against the same post-input view.
        let mut activation_vars = element_vars;
        if !input_updates.is_empty() {
            Arc::make_mut(&mut activation_vars).extend(input_updates);
        }

        // Start execution listeners (ADR 0037): before the element enacts its
        // own behaviour (creating a job, routing a gateway, opening a
        // sub-process) it runs a sequential chain of `start` listener jobs. The
        // element rests in ACTIVATING — `ElementActivated` was already emitted
        // above (nano's early-marker choice), but the *behaviour* is deferred
        // until the chain drains (see `advance_listener`). Listener-free
        // elements skip this entirely and fall straight through to
        // `run_activation_body`, so their journal is byte-identical.
        let start_listeners = self.listeners_of(
            instance_key,
            &element_id,
            crate::model::ListenerEventType::Start,
        );
        if let Some(first) = start_listeners.first() {
            let job_key = self.mint_key();
            let job_type = self.resolve_job_type(&activation_vars, &first.job_type);
            let retries = self.resolve_retries(&activation_vars, first.retries.as_deref());
            events.push(Event::ExecutionListenerJobCreated {
                job_key,
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
                job_type,
                event_type: crate::model::ListenerEventType::Start,
                listener_index: 0,
                scope,
                created_at: self.now,
                retries,
            });
            return (events, followups);
        }

        let (kind_events, kind_followups) = self.run_activation_body(
            instance_key,
            element_id,
            element_instance_key,
            scope,
            activation_vars,
        );
        events.extend(kind_events);
        (events, kind_followups)
    }

    /// Builds an `IO_MAPPING_ERROR` [`Event::IncidentRaised`] for a
    /// `zeebe:ioMapping` (input **or** output) whose source expression failed to
    /// evaluate (#939 / #946). A single taxonomy ([`state::IncidentKind::IoMapping`],
    /// REST `IO_MAPPING_ERROR`) covers every ioMapping failure on every element
    /// type — the element parks on the incident and the recovery is chosen by the
    /// `redrive` **phase**, not the incident kind (Zeebe parity: ioMapping
    /// failures re-drive uniformly by lifecycle phase in
    /// `BpmnVariableMappingBehavior`). The caller passes the
    /// [`state::IoMappingRedrive`] describing the lifecycle phase to replay when
    /// the incident is resolved.
    fn io_mapping_incident(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        failure: IoMappingFailure,
        redrive: state::IoMappingRedrive,
    ) -> Event {
        Event::IncidentRaised {
            incident_key: self.mint_key(),
            instance_key,
            element_instance_key,
            element_id,
            kind: state::IncidentKind::IoMapping,
            redrive: Some(redrive),
            reason: failure.reason,
            job_key: None,
            created_at: self.now,
        }
    }

    /// Runs an element's own activation behaviour — everything after
    /// `ElementActivated`: create the service/agent/user-task job, route the
    /// gateway, open the sub-process/ad-hoc scope, arm boundary events, or
    /// schedule an immediate `Complete`. Split out of [`activate`] so it can be
    /// deferred until the element's `start` execution-listener chain drains
    /// (ADR 0037). Listener-free elements call it inline with the same key/event
    /// ordering, so their journal is unchanged.
    fn run_activation_body(
        &mut self,
        instance_key: Key,
        element_id: String,
        element_instance_key: Key,
        scope: Key,
        element_vars: Arc<HashMap<String, Value>>,
    ) -> (Vec<Event>, Vec<Step>) {
        let kind = self.element_kind(instance_key, &element_id);
        let adhoc_def = self.adhoc_def_of(instance_key, &element_id);
        let mut events: Vec<Event> = Vec::new();
        let mut followups: Vec<Step> = Vec::new();

        match kind {
            // A service task creates a job and parks the token.
            Some(ElementKind::ServiceTask {
                job_type, priority, ..
            }) => {
                // A declarative ad-hoc container (Camunda BPMN_TASK /
                // `activeElementsCollection`) is NOT a job worker: on activation
                // it evaluates the FEEL collection to the inner element ids and
                // activates them directly — no job is minted, and the container
                // completes once those elements drain (ADR 0023 v1.1;
                // `AdHocSubProcessProcessor.readActivateElementsCollection`). The
                // agentic JOB_WORKER variant keeps the job-worker path below.
                let declarative_adhoc = adhoc_def
                    .as_ref()
                    .map(|d| d.impl_type == crate::model::AdHocImplementationType::BpmnTask)
                    .unwrap_or(false);
                if declarative_adhoc {
                    let def = adhoc_def
                        .as_ref()
                        .expect("declarative_adhoc implies adhoc_def is Some");
                    events.push(Event::AdHocActivated {
                        instance_key,
                        container_key: element_instance_key,
                        element_id: element_id.clone(),
                        output_collection: def.output_collection.clone(),
                        output_element: def.output_element.clone(),
                    });
                    let ids = def
                        .active_elements_collection
                        .as_deref()
                        .map(|expr| self.eval_adhoc_active_elements(expr, &element_vars, def))
                        .unwrap_or_default();
                    for id in &ids {
                        followups.push(Step::ActivateAdHocTool {
                            instance_key,
                            container_key: element_instance_key,
                            element_id: id.clone(),
                            variables: HashMap::new(),
                        });
                    }
                    // An empty collection has nothing to run: the container
                    // completes at once, exactly as Camunda completes an ad-hoc
                    // sub-process whose active-elements collection is empty.
                    if ids.is_empty() {
                        followups.push(Step::CompleteAdHoc {
                            instance_key,
                            container_key: element_instance_key,
                            cancel: false,
                        });
                    }
                    events.extend(self.arm_boundary_events(
                        instance_key,
                        element_instance_key,
                        scope,
                        &element_id,
                    ));
                    return (events, followups);
                }
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(&element_vars, &job_type);
                let priority = self.resolve_priority(&element_vars, priority.as_deref());
                let retries = self.resolve_retries(
                    &element_vars,
                    self.retries_of(instance_key, &element_id).as_deref(),
                );
                events.push(Event::JobCreated {
                    job_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    job_type,
                    created_at: self.now,
                    priority,
                    retries,
                });
                // An ad-hoc container: register its runtime state alongside the
                // agent job. The container element instance is the ad-hoc scope
                // (created above); its tool children are activated by the agent's
                // activate-element instructions on job completion (ADR 0023).
                if let Some(def) = &adhoc_def {
                    events.push(Event::AdHocActivated {
                        instance_key,
                        container_key: element_instance_key,
                        element_id: element_id.clone(),
                        output_collection: def.output_collection.clone(),
                        output_element: def.output_element.clone(),
                    });
                    // Advertise the tool catalog to the agent (Camunda
                    // `AdHocSubProcessProcessor.onActivate` writes the local var
                    // `adHocSubProcessElements`): a list of `{ elementId,
                    // elementName }`, one per activatable tool in document order,
                    // so the agent can discover which tools it may activate. It is
                    // written LOCAL to the container scope (registered in
                    // `activate`), so it rides the agent job's variable snapshot
                    // without leaking to the parent instance. Per-tool
                    // documentation / `zeebe:properties` / `fromAi` parameter
                    // schema are not parsed by nano yet (deferred; see #614).
                    let mut catalog_var = HashMap::new();
                    catalog_var.insert(
                        "adHocSubProcessElements".to_string(),
                        Value::List(Self::advertised_adhoc_catalog(def)),
                    );
                    events.push(Event::ScopedVariablesUpdated {
                        instance_key,
                        scope_key: element_instance_key,
                        variables: catalog_var,
                    });
                }
                // Arm timers/subscriptions for every attached boundary event.
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    scope,
                    &element_id,
                ));
            }
            // A user task creates a user task record and parks the token; a
            // CompleteUserTask releases it. The assignment/scheduling/priority
            // expressions declared on the element are resolved against the
            // element's scoped view at creation time.
            Some(ElementKind::UserTask(props)) => {
                let user_task_key = self.mint_key();
                let assignee = self
                    .resolve_user_task_string(&element_vars, props.assignee.as_deref())
                    .filter(|s| !s.is_empty());
                let candidate_groups =
                    self.resolve_user_task_list(&element_vars, props.candidate_groups.as_deref());
                let candidate_users =
                    self.resolve_user_task_list(&element_vars, props.candidate_users.as_deref());
                let due_date = self
                    .resolve_user_task_string(&element_vars, props.due_date.as_deref())
                    .filter(|s| !s.is_empty());
                let follow_up_date = self
                    .resolve_user_task_string(&element_vars, props.follow_up_date.as_deref())
                    .filter(|s| !s.is_empty());
                let priority = self.resolve_priority(&element_vars, props.priority.as_deref());
                // Resolve the form linkage, enforcing the Zeebe invariant that
                // `formId` and `externalReference` are mutually exclusive (an
                // external reference wins and suppresses `form_key` resolution).
                let (form_key, external_form_reference) =
                    self.resolve_user_task_form_linkage(&props);
                // An initial assignee (zeebe:assignmentDefinition) must fire the
                // `assigning` listeners exactly as a runtime assign does (Zeebe
                // parity: the assignee is stripped off the CREATED record and
                // routed through an assigning transition once the task is
                // available). With no assigning listeners the assignee stays on
                // CREATED, keeping listener-free/plain-assignee tasks
                // byte-identical.
                let has_assigning_listeners = !self
                    .task_listeners_of(
                        instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Assigning,
                    )
                    .is_empty();
                let route_initial_assignee = assignee.is_some() && has_assigning_listeners;
                let (created_assignee, deferred_initial_assignee) = if route_initial_assignee {
                    (None, assignee)
                } else {
                    (assignee, None)
                };
                events.push(Event::UserTaskCreated {
                    user_task_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    created_at: self.now,
                    assignee: created_assignee,
                    candidate_groups,
                    candidate_users,
                    due_date,
                    follow_up_date,
                    priority,
                    form_key,
                    external_form_reference,
                });
                // Creating task listeners (ADR 0037 §6): the task record exists
                // but is not yet available for work until the creating chain
                // drains. The pending transition blocks assign/complete/update
                // meanwhile. Listener-free user tasks skip this entirely.
                let creating = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Creating,
                );
                if let Some(first) = creating.first().cloned() {
                    let pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Creating,
                        // Carried through the creating chain, then routed into an
                        // assigning transition when creating drains.
                        assignee: deferred_initial_assignee,
                        update: None,
                        variables: std::collections::HashMap::new(),
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    events.extend(self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Creating,
                        pending,
                        &first,
                    ));
                } else if let Some(initial) = deferred_initial_assignee {
                    // No creating listeners: the task is available at once, so the
                    // initial assignee's assigning transition starts immediately.
                    let assigning = self.task_listeners_of(
                        instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Assigning,
                    );
                    if let Some(first) = assigning.first().cloned() {
                        let pending = state::PendingUserTaskTransition {
                            event_type: crate::model::TaskListenerEventType::Assigning,
                            assignee: Some(initial),
                            update: None,
                            variables: std::collections::HashMap::new(),
                            corrections: crate::model::UserTaskCorrections::default(),
                        };
                        events.extend(self.start_task_listener_chain(
                            user_task_key,
                            instance_key,
                            element_instance_key,
                            &element_id,
                            crate::model::TaskListenerEventType::Assigning,
                            pending,
                            &first,
                        ));
                    }
                }
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    scope,
                    &element_id,
                ));
            }
            // A timer intermediate catch event arms a timer and parks the token;
            // a clock tick (TriggerTimers) releases it once the timer is due.
            Some(ElementKind::TimerIntermediateCatchEvent { duration_millis }) => {
                let timer_key = self.mint_key();
                let timer_def = self.timer_def_of(instance_key, &element_id);
                let (due_at, _) = self.resolve_timer(
                    &element_vars,
                    timer_def.as_ref(),
                    self.now,
                    duration_millis,
                );
                events.push(Event::TimerCreated {
                    timer_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    due_at,
                    kind: state::TimerKind::IntermediateCatch,
                });
            }
            // A message intermediate catch event opens a subscription and parks
            // the token; a matching CorrelateMessage releases it.
            Some(ElementKind::MessageIntermediateCatchEvent {
                message_name,
                correlation_key,
            }) => {
                let subscription_key = self.mint_key();
                // The message name may be a FEEL expression evaluated on
                // activation against the instance variables (Zeebe parity).
                let message_name = self.resolve_event_name(&element_vars, &message_name);
                // A declared correlation-key expression that fails to evaluate
                // (missing variable, `null`, or a type error like `str + null`)
                // must NOT silently open a subscription with an empty key — that
                // key can never be matched by a published message, so the token
                // would park forever with no error. Raise an ExpressionEvaluation
                // incident instead (Zeebe parity); resolving it re-opens the
                // subscription (see the `ResolveIncident` re-drive).
                match self.resolve_correlation_value_checked(&element_vars, &correlation_key) {
                    Err(reason) => {
                        events.push(Event::IncidentRaised {
                            incident_key: subscription_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                            kind: state::IncidentKind::ExpressionEvaluation,
                            redrive: None,
                            reason,
                            job_key: None,
                            created_at: self.now,
                        });
                    }
                    Ok(correlation_value) => {
                        let kind = state::MessageSubscriptionKind::IntermediateCatch;
                        // Zeebe-style placement: the canonical subscription lives on the
                        // partition owning `hash(correlation_key)`. When that is this
                        // partition (always so single-partition), open it locally; else
                        // park the token on an `Opening` record and let the host route an
                        // `OpenMessageSubscription` to the message partition.
                        if self.subscription_partition(&correlation_value) == self.partition_id {
                            events.push(Event::MessageSubscriptionCreated {
                                subscription_key,
                                instance_key,
                                element_instance_key,
                                element_id,
                                message_name,
                                correlation_key: correlation_value,
                                kind,
                            });
                        } else {
                            events.push(Event::MessageSubscriptionOpening {
                                subscription_key,
                                instance_key,
                                element_instance_key,
                                element_id,
                                message_name,
                                correlation_key: correlation_value,
                                kind,
                            });
                        }
                    }
                }
            }
            // A signal intermediate catch event opens a signal subscription
            // (name-only, no correlation key) and parks the token; a matching
            // BroadcastSignal releases it.
            Some(ElementKind::SignalIntermediateCatchEvent { signal_name }) => {
                let subscription_key = self.mint_key();
                // The signal name may be a FEEL expression evaluated on
                // activation against the instance variables (Zeebe parity).
                let signal_name = self.resolve_event_name(&element_vars, &signal_name);
                events.push(Event::SignalSubscriptionCreated {
                    subscription_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    signal_name,
                    kind: state::MessageSubscriptionKind::IntermediateCatch,
                });
            }
            // A conditional intermediate catch event evaluates its FEEL condition
            // on arrival: if already `true` the token passes straight through;
            // otherwise it parks on a conditional subscription, re-evaluated on
            // each change to a variable the condition references. A condition that
            // errors (e.g. a non-boolean result while a referenced variable is not
            // yet set) is treated as not-yet-satisfied and simply waits — a
            // conditional event is a wait-until, not an incident (a deliberate
            // flat-scope choice).
            Some(ElementKind::ConditionalIntermediateCatchEvent { condition }) => {
                let vars = self.variables(instance_key);
                if matches!(crate::feel::eval_bool(&condition, &vars), Ok(true)) {
                    followups.push(Step::Complete {
                        instance_key,
                        element_instance_key,
                        element_id,
                    });
                } else {
                    let subscription_key = self.mint_key();
                    let referenced_vars = sorted_referenced_vars(&condition);
                    events.push(Event::ConditionalSubscriptionCreated {
                        subscription_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        condition,
                        referenced_vars,
                        kind: state::MessageSubscriptionKind::IntermediateCatch,
                    });
                }
            }
            // An embedded sub-process opens a token scope: it activates its inner
            // start event inside its own scope (this element instance) and rests
            // while the inner flow runs. It completes once the scope drains (see
            // `complete_drained_subprocesses`) or is interrupted by an error
            // boundary.
            Some(ElementKind::SubProcess { start_event }) => {
                // A sub-process can also carry timer/message boundary events;
                // arm them just like a service task's.
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    scope,
                    &element_id,
                ));
                followups.push(Step::Activate {
                    instance_key,
                    element_id: start_event,
                    scope: element_instance_key,
                    via: None,
                });
            }
            // A call activity spawns a distinct **child process instance** of its
            // `calledElement` (Zeebe parity) and parks its own token in ACTIVATED
            // while the child runs. The child carries `parentProcessInstanceKey`/
            // `parentElementInstanceKey` back to this element instance so tooling
            // can draw the parent↔child tree. Variable propagation across the
            // instance boundary defaults to Zeebe's full-scope behaviour: the
            // whole parent scope seeds the child on spawn and the whole child
            // scope merges back on completion. `propagateAllParentVariables=false`
            // suppresses the parent→child copy (only the input mappings seed the
            // child) and `propagateAllChildVariables=false` suppresses the
            // child→parent copy (only the output mappings merge back); input
            // mappings (seeding the child, isolated scope) and, on child
            // completion, output mappings (see `complete_call_activity`) are
            // applied on top of whichever propagation the flags leave enabled. The parent token completes when the
            // child finishes (see `complete_finished_instances`); cancelling the
            // parent cancels the in-flight child (see `cascade_cancel_children`).
            Some(ElementKind::CallActivity {
                called_process_id,
                propagate_all_parent_variables,
                ..
            }) => {
                // Boundary events on the call activity are armed like any other
                // activity (timers/messages interrupt the wait for the child).
                events.extend(self.arm_boundary_events(
                    instance_key,
                    element_instance_key,
                    scope,
                    &element_id,
                ));
                let (spawn_events, spawn_followups) = self.spawn_call_activity_child(
                    instance_key,
                    element_instance_key,
                    &element_id,
                    &called_process_id,
                    propagate_all_parent_variables,
                    &element_vars,
                );
                events.extend(spawn_events);
                followups.extend(spawn_followups);
            }
            // A compensation throw event triggers compensation of the completed
            // compensable activities in its scope, running each one's handler,
            // and rests until they finish. With nothing to compensate it is a
            // pass-through.
            Some(ElementKind::CompensationThrowEvent) => {
                let targets = self.compensable_in_scope(instance_key, scope);
                if targets.is_empty() {
                    followups.push(Step::Complete {
                        instance_key,
                        element_instance_key,
                        element_id,
                    });
                } else {
                    // Compensate newest-first (reverse completion order).
                    let mut handlers = Vec::new();
                    let mut consumed = Vec::new();
                    for target in targets.iter().rev() {
                        handlers.push(target.handler.clone());
                        consumed.push(target.element_instance_key);
                    }
                    events.push(Event::CompensationTriggered {
                        instance_key,
                        throw_element_instance_key: element_instance_key,
                        throw_element_id: element_id.clone(),
                        scope,
                        handlers: handlers.clone(),
                        consumed,
                    });
                    for handler in handlers {
                        followups.push(Step::Activate {
                            instance_key,
                            element_id: handler,
                            scope,
                            via: None,
                        });
                    }
                }
            }
            // An inline-FEEL script task is a synchronous activity: it activates
            // and immediately completes (no job). Its FEEL expression is
            // evaluated at completion (see `complete`), where a failure raises
            // an ExpressionEvaluation incident that re-evaluates on resolution.
            // Pass-through elements (events, exclusive gateway, parallel split)
            // complete immediately; routing happens at completion.
            Some(_) => {
                followups.push(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id,
                });
            }
            // Unknown element / instance: nothing to do.
            None => {}
        }

        (events, followups)
    }

    /// Opens a multi-instance body: evaluates the input collection and fans out
    /// one child of the activity per item. A non-list result or an evaluation
    /// error yields an empty loop (the body completes immediately) — a flat-scope
    /// choice mirroring the conditional-event error semantics. Children run in the
    /// body's scope; parallel bodies spawn every child at once, sequential bodies
    /// spawn the first and chain the rest at each child's completion.
    fn activate_multi_instance_body(
        &mut self,
        instance_key: Key,
        element_id: String,
        scope: Key,
        mi: crate::model::MultiInstance,
    ) -> (Vec<Event>, Vec<Step>) {
        let body_key = self.mint_key();
        let mut events = vec![
            Event::ElementActivating {
                instance_key,
                element_instance_key: body_key,
                element_id: element_id.clone(),
            },
            Event::ElementActivated {
                instance_key,
                element_instance_key: body_key,
                element_id: element_id.clone(),
                scope,
            },
            // The multi-instance body is a variable scope: its children run inside
            // it and its `outputCollection` accumulates here.
            Event::VariableScopeCreated {
                instance_key,
                scope_key: body_key,
                parent_scope_key: scope,
            },
        ];
        let (fanout_events, followups) =
            self.run_mi_body_activation(instance_key, element_id, body_key, scope, mi);
        events.extend(fanout_events);
        (events, followups)
    }

    /// Runs a multi-instance body's activation body: applies the activity's own
    /// input mappings (local to the body scope), evaluates the input collection,
    /// emits `MultiInstanceActivated`, gates on the body's `start` listeners, and
    /// fans out the children. Split out of [`activate_multi_instance_body`] so a
    /// resolved body input-mapping incident can re-drive it
    /// ([`Step::RetryMiBodyActivation`]) without re-emitting the body's
    /// `ElementActivating`/`ElementActivated`/`VariableScopeCreated`.
    ///
    /// The body evaluates the activity's mappings only to feed the input
    /// collection; the mappings are authoritatively applied — and their eval
    /// failures raised as incidents — per child in [`activate_mi_child`], where
    /// `inputElement`/`loopCounter` are bound. A body-level mapping that fails
    /// **because** it references those not-yet-bound per-child bindings is
    /// *tolerated* (skipped); a mapping that fails for any other reason is a
    /// *genuine* failure and raises an `IO_MAPPING_ERROR` incident rather than
    /// silently projecting an empty collection (#946).
    fn run_mi_body_activation(
        &mut self,
        instance_key: Key,
        element_id: String,
        body_key: Key,
        scope: Key,
        mi: crate::model::MultiInstance,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut events = Vec::new();
        // Input mappings on the activity apply once, on body activation, LOCAL to
        // the body scope (Zeebe semantics), so they feed the input collection and
        // the children without leaking to the parent scope. Their source
        // expressions are evaluated against the body's *enclosing* scope view, so
        // a multi-instance activity nested in a sub-process can read that
        // sub-process's locals.
        let enclosing_vars = self.variables_for_element(instance_key, scope);
        // A CALL ACTIVITY's `zeebe:input` mappings seed only its isolated child
        // PROCESS scope — they are applied once in `spawn_call_activity_child` per
        // child, never local to the MI body scope. Applying them here would leak
        // the callee's own input-mapped locals into the body scope (which every
        // child inherits), so the callee-id `=calledElement` expression and the
        // per-child spawn would see them and a self-referencing mapping would
        // compound across the body → child re-evaluation (#1175). Mirrors the
        // normal activation path (`activate_body`), which likewise skips a call
        // activity's inputs. The input collection is evaluated without them.
        let is_call_activity = matches!(
            self.element_kind(instance_key, &element_id),
            Some(ElementKind::CallActivity { .. })
        );
        let inputs = if is_call_activity {
            Vec::new()
        } else {
            self.io_inputs(instance_key, &element_id)
        };
        let input_updates = if inputs.is_empty() {
            HashMap::new()
        } else {
            // Tolerate a mapping that fails because it references a per-child
            // binding (`loopCounter` / the configured `inputElement`) absent at the
            // body level — it is applied authoritatively per child. A mapping that
            // fails for any other reason is a genuine failure: park the body on an
            // `IO_MAPPING_ERROR` incident whose resolution re-drives this body
            // activation (#946).
            let mut tolerated = std::collections::HashSet::new();
            tolerated.insert("loopCounter".to_string());
            if let Some(name) = &mi.input_element {
                tolerated.insert(name.clone());
            }
            match self.eval_io_mappings_tolerating(&enclosing_vars, &inputs, &tolerated) {
                Ok(updates) => updates,
                Err(failure) => {
                    let event = self.io_mapping_incident(
                        instance_key,
                        body_key,
                        element_id,
                        failure,
                        state::IoMappingRedrive::MiBodyActivation,
                    );
                    return (vec![event], Vec::new());
                }
            }
        };
        if !input_updates.is_empty() {
            events.push(Event::ScopedVariablesUpdated {
                instance_key,
                scope_key: body_key,
                variables: input_updates.clone(),
            });
        }
        // The input collection is evaluated in the body scope view (the enclosing
        // scope with any body input mappings overlaid).
        let vars: Arc<HashMap<String, Value>> = {
            if input_updates.is_empty() {
                enclosing_vars
            } else {
                let mut merged = (*enclosing_vars).clone();
                merged.extend(input_updates);
                Arc::new(merged)
            }
        };
        let items: Vec<Value> = match crate::feel::eval(&mi.input_collection, &vars) {
            Ok(Value::List(list)) => list,
            _ => Vec::new(),
        };
        let total = items.len();
        let sequential = mi.sequential;
        events.push(Event::MultiInstanceActivated {
            instance_key,
            body_key,
            element_id: element_id.clone(),
            sequential,
            items,
            input_element: mi.input_element.clone(),
            output_collection: mi.output_collection.clone(),
            output_element: mi.output_element.clone(),
            completion_condition: mi.completion_condition.clone(),
        });

        // Arm timers/subscriptions for every boundary event attached to the
        // multi-instance activity, ON THE BODY. A boundary on an MI activity
        // interrupts the whole loop, so it belongs to the body (whose flow scope
        // is the activity's enclosing scope — giving the boundary's outgoing flow
        // the correct level) rather than to any one child. Firing routes through
        // `interrupt_activity_via_boundary`, which recognises the body and tears
        // down every active child plus the body record (#1170). Armed here, before
        // the start-listener gate below, so the boundary is live for the whole
        // life of the activity. A body with no attached boundaries yields no
        // events (the common case), so this is a no-op for every ordinary loop.
        events.extend(self.arm_boundary_events(instance_key, body_key, scope, &element_id));

        // Start-listener gate (ADR 0037): the body rests in ACTIVATING while its
        // `start` chain runs. Zeebe fires the activity's start listeners at the
        // body boundary, before any child is instantiated, so child spawning is
        // deferred to `advance_listener` (Start), which re-derives it via
        // `spawn_multi_instance_children` once the chain drains (by which point
        // the `MultiInstanceActivated` event above has been applied). Listener-free
        // bodies fan out inline, byte-identical to before.
        let start_listeners = self.listeners_of(
            instance_key,
            &element_id,
            crate::model::ListenerEventType::Start,
        );
        if let Some(first) = start_listeners.first() {
            let job_key = self.mint_key();
            let job_type = self.resolve_job_type(&vars, &first.job_type);
            let retries = self.resolve_retries(&vars, first.retries.as_deref());
            events.push(Event::ExecutionListenerJobCreated {
                job_key,
                instance_key,
                element_instance_key: body_key,
                element_id: element_id.clone(),
                job_type,
                event_type: crate::model::ListenerEventType::Start,
                listener_index: 0,
                scope,
                created_at: self.now,
                retries,
            });
            return (events, Vec::new());
        }

        let mut followups = Vec::new();
        if total == 0 {
            followups.push(Step::CompleteMiBody {
                instance_key,
                body_key,
            });
        } else if sequential {
            followups.push(Step::ActivateMiChild {
                instance_key,
                element_id,
                body_key,
                index: 0,
            });
        } else {
            for index in 0..total {
                followups.push(Step::ActivateMiChild {
                    instance_key,
                    element_id: element_id.clone(),
                    body_key,
                    index,
                });
            }
        }
        (events, followups)
    }

    /// Fans out a multi-instance body's children (or completes an empty body),
    /// re-derived from the body record: an empty loop completes the body, a
    /// sequential loop starts its first child, a parallel loop starts one child
    /// per item. Split out of [`activate_multi_instance_body`] so the fan-out can
    /// be deferred behind the body's `start` execution-listener chain (ADR 0037)
    /// and resumed from [`advance_listener`] once it drains.
    fn spawn_multi_instance_children(&self, instance_key: Key, body_key: Key) -> Vec<Step> {
        let (element_id, sequential, total) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body_key))
        {
            Some(mi) => (mi.element_id.clone(), mi.sequential, mi.items.len()),
            None => return Vec::new(),
        };
        let mut followups = Vec::new();
        if total == 0 {
            followups.push(Step::CompleteMiBody {
                instance_key,
                body_key,
            });
        } else if sequential {
            followups.push(Step::ActivateMiChild {
                instance_key,
                element_id,
                body_key,
                index: 0,
            });
        } else {
            for index in 0..total {
                followups.push(Step::ActivateMiChild {
                    instance_key,
                    element_id: element_id.clone(),
                    body_key,
                    index,
                });
            }
        }
        followups
    }

    /// Activates one child of a multi-instance body: instantiates the activity
    /// again in the body scope, binding the `index`-th item (as `inputElement`,
    /// when named) and the 1-based `loopCounter` into the child's local variable
    /// overlay. A service-task child creates a job; any other activity kind passes
    /// straight through to completion (which routes back into the loop).
    fn activate_mi_child(
        &mut self,
        instance_key: Key,
        element_id: String,
        body_key: Key,
        index: usize,
    ) -> (Vec<Event>, Vec<Step>) {
        let (item, input_element) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body_key))
        {
            Some(mi) => (
                mi.items.get(index).cloned().unwrap_or(Value::Null),
                mi.input_element.clone(),
            ),
            None => return (Vec::new(), Vec::new()),
        };

        let child_key = self.mint_key();
        let mut locals: HashMap<String, Value> = HashMap::new();
        if let Some(name) = &input_element {
            locals.insert(name.clone(), item);
        }
        locals.insert("loopCounter".to_string(), Value::Int((index as i64) + 1));

        // Seed the ad-hoc container's `outputCollection` as an empty local list
        // BEFORE the per-child input mappings are applied below — mirroring the
        // normal activation path (`activate_body`), which seeds the collection
        // before running the container's inputs — so a (nonsensical but possible)
        // per-child `zeebe:input` targeting the outputCollection variable
        // overrides the seed instead of being clobbered by a seed applied
        // afterwards (#1175). Seeded into the child's `locals` (carried in
        // `MultiInstanceChildActivated.local_variables`, which replaces the child
        // scope wholesale on apply) rather than a discrete post-activation
        // `ScopedVariablesUpdated`: that establishes it with the child scope, so
        // the mappings that follow can still override it and `run_mi_child_behaviour`
        // need not re-seed it. Applies to both the JOB_WORKER and declarative
        // (BPMN_TASK) ad-hoc variants.
        if let Some(name) = self
            .adhoc_def_of(instance_key, &element_id)
            .and_then(|d| d.output_collection.clone())
        {
            locals.insert(name, Value::List(Vec::new()));
        }

        // The scoped view this child evaluates its own FEEL attributes (job type,
        // retries, priority) against: the multi-instance body's scope (already
        // applied) overlaid with the child's own `inputElement`/`loopCounter`
        // bindings (not yet applied — carried in `locals`). So a child job type
        // like `="worker-" + loopCounter` resolves correctly.
        let mut child_vars = (*self.variables_for_element(instance_key, body_key)).clone();
        child_vars.extend(locals.clone());

        // Apply the activity's own input mappings (`zeebe:input`) per-child,
        // evaluated with `inputElement`/`loopCounter` already bound, writing the
        // results LOCAL to this child's scope — matching Zeebe, which applies an
        // MI inner activity's input mappings on each instance's activation into
        // that instance's OWN scope (`getVariableScopeKey` returns the
        // element-instance key while the loop counter is set). `activate_mi_child`
        // hand-builds the child (bypassing `activate`), so unlike a
        // normally-activated element these mappings must be applied here; they are
        // then visible both to the child's job-type/retry FEEL resolution below
        // and, for a sub-process child, to its inner flow.
        //
        // A call-activity MI child's input mappings seed only its isolated child
        // PROCESS scope — never the child element's own MI scope — so they are NOT
        // applied here: `spawn_call_activity_child` (invoked by
        // `run_mi_child_behaviour`) evaluates them exactly once into the child
        // seed. Applying them here as well would evaluate them twice (a second
        // application that compounds for a mapping referencing an earlier mapped
        // target) and would let the callee-id `=calledElement` expression see the
        // callee's own input-mapped locals (#1175). Mirrors the normal activation
        // path, which likewise routes a call activity's inputs through the spawn.
        let is_call_activity = matches!(
            self.element_kind(instance_key, &element_id),
            Some(ElementKind::CallActivity { .. })
        );
        let inputs = if is_call_activity {
            Vec::new()
        } else {
            self.io_inputs(instance_key, &element_id)
        };
        if !inputs.is_empty() {
            match self.eval_io_mappings_in(&child_vars, &inputs) {
                Ok(mut mapped) => {
                    // `loopCounter` is a reserved MI binding. The child's
                    // output-collection index is now engine-owned runtime state
                    // (`MultiInstanceState::child_indices`, read back by
                    // `complete_mi_child`), so a clobbered counter can no longer
                    // misindex a child's output. We still drop any user
                    // `zeebe:input` mapping targeting `loopCounter` so the
                    // FEEL-visible reserved binding (job type, `outputElement`,
                    // etc.) keeps reporting the true engine-owned counter rather
                    // than a mapped-over value.
                    mapped.remove("loopCounter");
                    child_vars.extend(mapped.clone());
                    locals.extend(mapped);
                }
                Err(failure) => {
                    // A per-child input mapping that fails to evaluate halts the
                    // child with an `IO_MAPPING_ERROR` incident rather than running
                    // it against a silently-unset variable (#939/#946). The child
                    // is activated and parked on the incident so the body waits for
                    // it; resolution re-drives the child's *activation* phase
                    // (`MiChildActivation`), re-applying the now-fixed inputs and
                    // re-enacting its behaviour for the same child instance —
                    // without re-emitting its activation events.
                    let mut events = vec![
                        Event::ElementActivating {
                            instance_key,
                            element_instance_key: child_key,
                            element_id: element_id.clone(),
                        },
                        Event::ElementActivated {
                            instance_key,
                            element_instance_key: child_key,
                            element_id: element_id.clone(),
                            scope: body_key,
                        },
                        Event::MultiInstanceChildActivated {
                            instance_key,
                            body_key,
                            child_key,
                            index,
                            local_variables: locals,
                        },
                    ];
                    let event = self.io_mapping_incident(
                        instance_key,
                        child_key,
                        element_id,
                        failure,
                        state::IoMappingRedrive::MiChildActivation { body_key, index },
                    );
                    events.push(event);
                    return (events, Vec::new());
                }
            }
        }

        let events = vec![
            Event::ElementActivating {
                instance_key,
                element_instance_key: child_key,
                element_id: element_id.clone(),
            },
            Event::ElementActivated {
                instance_key,
                element_instance_key: child_key,
                element_id: element_id.clone(),
                scope: body_key,
            },
            Event::MultiInstanceChildActivated {
                instance_key,
                body_key,
                child_key,
                index,
                local_variables: locals,
            },
        ];
        let (behaviour_events, followups) =
            self.run_mi_child_behaviour(instance_key, element_id, body_key, child_key, &child_vars);
        let mut events = events;
        events.extend(behaviour_events);
        (events, followups)
    }

    /// Runs a multi-instance child's own behaviour once its `inputElement` /
    /// `loopCounter` bindings and per-child input mappings have been applied
    /// (`child_vars` is the child's resolved scope view): a service-task child
    /// mints its job (registering an ad-hoc runtime record too when the activity
    /// is a JOB_WORKER ad-hoc container), an embedded-sub-process child opens its
    /// scope and activates its inner start event, a call-activity child spawns its
    /// own child process instance, any other kind passes straight through to
    /// completion. Shared by the first activation ([`activate_mi_child`]) and the
    /// incident re-drive ([`retry_mi_child_activation`]) so both enact identical
    /// behaviour. Boundary events on the multi-instance activity are armed on the
    /// body, not here (see [`run_mi_body_activation`]).
    fn run_mi_child_behaviour(
        &mut self,
        instance_key: Key,
        element_id: String,
        _body_key: Key,
        child_key: Key,
        child_vars: &HashMap<String, Value>,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut events = Vec::new();
        let mut followups = Vec::new();
        // Boundary events attached to a multi-instance activity are armed once, on
        // the BODY (`run_mi_body_activation`), never per child — a boundary on the
        // activity interrupts the whole loop. So no child branch arms them.
        match self.element_kind(instance_key, &element_id) {
            Some(ElementKind::ServiceTask {
                job_type, priority, ..
            }) => {
                let adhoc_def = self.adhoc_def_of(instance_key, &element_id);
                // A DECLARATIVE ad-hoc container (Camunda BPMN_TASK /
                // `activeElementsCollection`) used as an MI child is NOT a job
                // worker: because an ad-hoc sub-process flattens to
                // `ElementKind::ServiceTask`, minting a job here would spawn a
                // stray agent job and never evaluate its active-elements
                // collection. Mirror the normal activation path
                // (`run_activation_body`): on activation evaluate the FEEL
                // collection to inner element ids and activate them directly — no
                // job — and let them drain back through `complete_adhoc_container`'s
                // MI-child route into `complete_mi_child` (#1175). The container's
                // `outputCollection` is seeded ahead of the per-child input
                // mappings in `activate_mi_child`, so it is not re-seeded here.
                let declarative_adhoc = adhoc_def
                    .as_ref()
                    .map(|d| d.impl_type == crate::model::AdHocImplementationType::BpmnTask)
                    .unwrap_or(false);
                if declarative_adhoc {
                    let def = adhoc_def
                        .as_ref()
                        .expect("declarative_adhoc implies adhoc_def is Some");
                    events.push(Event::AdHocActivated {
                        instance_key,
                        container_key: child_key,
                        element_id: element_id.clone(),
                        output_collection: def.output_collection.clone(),
                        output_element: def.output_element.clone(),
                    });
                    // Evaluate the active-elements collection against the child's
                    // resolved scope view (`inputElement`/`loopCounter` bound) and
                    // activate each named tool into THIS child's container scope.
                    let ids = def
                        .active_elements_collection
                        .as_deref()
                        .map(|expr| self.eval_adhoc_active_elements(expr, child_vars, def))
                        .unwrap_or_default();
                    for id in &ids {
                        followups.push(Step::ActivateAdHocTool {
                            instance_key,
                            container_key: child_key,
                            element_id: id.clone(),
                            variables: HashMap::new(),
                        });
                    }
                    // An empty collection has nothing to run: the container
                    // completes at once, routing straight into the loop via
                    // `complete_adhoc_container`'s MI-child branch.
                    if ids.is_empty() {
                        followups.push(Step::CompleteAdHoc {
                            instance_key,
                            container_key: child_key,
                            cancel: false,
                        });
                    }
                    return (events, followups);
                }
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(child_vars, &job_type);
                let priority = self.resolve_priority(child_vars, priority.as_deref());
                let retries = self.resolve_retries(
                    child_vars,
                    self.retries_of(instance_key, &element_id).as_deref(),
                );
                events.push(Event::JobCreated {
                    job_key,
                    instance_key,
                    element_instance_key: child_key,
                    element_id: element_id.clone(),
                    job_type,
                    created_at: self.now,
                    priority,
                    retries,
                });
                // A multi-instance child that is a JOB_WORKER ad-hoc container
                // (`adHocSubProcess` with a `zeebe:taskDefinition`) registers its
                // own ad-hoc runtime record alongside the agent job — exactly like
                // a normally-activated container (see `run_activation_body`) — so
                // the agent can activate tools into THIS child's scope and an
                // interrupting boundary on the loop can tear that scope down
                // (#1170). The container element instance is the ad-hoc scope
                // (`child_key`); its tool catalog is advertised local to it. The
                // container's `outputCollection` is seeded ahead of the per-child
                // input mappings in `activate_mi_child` (#1175), so it is not
                // re-seeded here.
                if let Some(def) = &adhoc_def {
                    events.push(Event::AdHocActivated {
                        instance_key,
                        container_key: child_key,
                        element_id: element_id.clone(),
                        output_collection: def.output_collection.clone(),
                        output_element: def.output_element.clone(),
                    });
                    let mut catalog_var = HashMap::new();
                    catalog_var.insert(
                        "adHocSubProcessElements".to_string(),
                        Value::List(Self::advertised_adhoc_catalog(def)),
                    );
                    events.push(Event::ScopedVariablesUpdated {
                        instance_key,
                        scope_key: child_key,
                        variables: catalog_var,
                    });
                }
            }
            // A multi-instance child that is an embedded SUB-PROCESS opens its own
            // token scope (this child element instance) and activates its inner
            // start event inside it — exactly like a normally-activated
            // sub-process (see `activate`). The child rests while its inner flow
            // runs; once that scope drains, `complete_drained_subprocesses`
            // recognises the drained instance as a multi-instance child and routes
            // it back into the loop via `complete_mi_child` (output collection +
            // join / next child) rather than following the activity's outgoing
            // flow. This is what makes the nested "wave" pattern — a sequential MI
            // over waves wrapping a parallel MI over a wave's tasks — executable.
            Some(ElementKind::SubProcess { start_event }) => {
                followups.push(Step::Activate {
                    instance_key,
                    element_id: start_event,
                    scope: child_key,
                    via: None,
                });
            }
            // A multi-instance child that is a CALL ACTIVITY parks its token on a
            // distinct spawned child process instance (Zeebe parity), one per
            // loop item — exactly like a normally-activated call activity (see
            // `run_activation_body`). When the child process drains,
            // `complete_call_activity` routes the loop back through
            // `complete_mi_child`. An interrupting boundary on the loop cancels
            // every one of these child instances via the body teardown (#1170).
            Some(ElementKind::CallActivity {
                called_process_id,
                propagate_all_parent_variables,
                ..
            }) => {
                let (spawn_events, spawn_followups) = self.spawn_call_activity_child(
                    instance_key,
                    child_key,
                    &element_id,
                    &called_process_id,
                    propagate_all_parent_variables,
                    child_vars,
                );
                events.extend(spawn_events);
                followups.extend(spawn_followups);
            }
            _ => {
                followups.push(Step::Complete {
                    instance_key,
                    element_instance_key: child_key,
                    element_id,
                });
            }
        }
        (events, followups)
    }

    /// Re-drives a multi-instance child's *activation* phase after its
    /// input-mapping incident is resolved (#946): the child element instance is
    /// already ACTIVATED (its `ElementActivating`/`ElementActivated`/
    /// `MultiInstanceChildActivated` were emitted and are not re-emitted). Reads
    /// the child's already-bound scope (which carries `inputElement`/`loopCounter`
    /// from `MultiInstanceChildActivated`), re-applies the now-fixed input
    /// mappings — writing them into the child scope — and re-enacts the child's
    /// behaviour, exactly as a clean first activation would have. A mapping that
    /// still fails re-raises the same incident.
    fn retry_mi_child_activation(
        &mut self,
        instance_key: Key,
        element_id: String,
        body_key: Key,
        child_key: Key,
        index: usize,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut child_vars = (*self.variables_for_element(instance_key, child_key)).clone();
        let mut events = Vec::new();
        let inputs = self.io_inputs(instance_key, &element_id);
        if !inputs.is_empty() {
            match self.eval_io_mappings_in(&child_vars, &inputs) {
                Ok(mut mapped) => {
                    // `loopCounter` stays engine-owned (see `activate_mi_child`).
                    mapped.remove("loopCounter");
                    if !mapped.is_empty() {
                        child_vars.extend(mapped.clone());
                        events.push(Event::ScopedVariablesUpdated {
                            instance_key,
                            scope_key: child_key,
                            variables: mapped,
                        });
                    }
                }
                Err(failure) => {
                    let event = self.io_mapping_incident(
                        instance_key,
                        child_key,
                        element_id,
                        failure,
                        state::IoMappingRedrive::MiChildActivation { body_key, index },
                    );
                    return (vec![event], Vec::new());
                }
            }
        }
        let (behaviour_events, followups) =
            self.run_mi_child_behaviour(instance_key, element_id, body_key, child_key, &child_vars);
        events.extend(behaviour_events);
        (events, followups)
    }

    /// Completes one multi-instance child: collects its `output_element` (in the
    /// child's local scope) into the body's results at the child's index, then
    /// decides what comes next — fire the completion condition (complete the body
    /// early), spawn the next child (sequential), or complete the body once every
    /// child has finished (parallel).
    ///
    /// `output_overlay` carries the produced outputs of a child whose results do
    /// NOT live in its own element-instance scope: a CALL-ACTIVITY child parks its
    /// token on a distinct callee process instance, so its propagated child
    /// variables (and the activity's output-mapping result) are passed here as an
    /// already-projected overlay (the caller, `complete_call_activity`, applies the
    /// activity's `zeebe:output` itself against the callee variables). When `Some`,
    /// the overlay is laid over the child's scope view for the `output_element`
    /// evaluation and the element's own `zeebe:output` mappings are NOT re-applied
    /// here (the caller already did). When `None` (service-task / sub-process
    /// children, whose outputs are already resident in the child scope), the
    /// element's `zeebe:output` mappings are applied in-memory as before.
    fn complete_mi_child(
        &mut self,
        instance_key: Key,
        child_eik: Key,
        element_id: String,
        body_key: Key,
        output_overlay: Option<HashMap<String, Value>>,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut events = vec![
            Event::ElementCompleting {
                instance_key,
                element_instance_key: child_eik,
                element_id: element_id.clone(),
            },
            Event::ElementCompleted {
                instance_key,
                element_instance_key: child_eik,
                element_id: element_id.clone(),
            },
        ];

        // Snapshot the loop configuration and this child's index before evaluating.
        let (
            output_element,
            completion_condition,
            sequential,
            total,
            spawned,
            active_now,
            mi_element,
        ) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body_key))
        {
            Some(mi) => (
                mi.output_element.clone(),
                mi.completion_condition.clone(),
                mi.sequential,
                mi.items.len(),
                mi.spawned,
                mi.active.len(),
                mi.element_id.clone(),
            ),
            None => return (events, Vec::new()),
        };
        let index = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body_key))
            .and_then(|mi| mi.child_indices.get(&child_eik))
            .copied()
            // Fallback for instances rehydrated from a pre-`child_indices`
            // snapshot: derive from the child's `loopCounter` binding. Newly
            // activated children always hit the authoritative map above, so no
            // write path into the child scope can corrupt the index.
            .or_else(|| {
                self.state
                    .instances
                    .get(&instance_key)
                    .and_then(|i| i.scope_variables.get(&child_eik))
                    .and_then(|l| l.get("loopCounter"))
                    .and_then(|v| v.as_f64())
                    .map(|c| (c as i64 - 1).max(0) as usize)
            })
            .unwrap_or(0);

        // Collect this child's output (evaluated in its local scope) at its index.
        // First apply the MI element's own output mappings (`zeebe:output`) to the
        // child's local scope view, matching Zeebe: an MI inner activity's output
        // mappings are applied on each instance's completion into the instance's
        // OWN scope (`getVariableScopeKey` returns the element-instance key while
        // the loop counter is set), so `outputElement` can read them. They are NOT
        // propagated to the parent — only the aggregated `outputCollection` is. We
        // therefore overlay the mapped values onto the eval context in memory
        // rather than writing them into the (about-to-be-torn-down, non-propagated)
        // child scope.
        let output = match output_element.as_deref() {
            None => None,
            Some(expr) => {
                let visible = self.variables_for_element(instance_key, child_eik);
                match &output_overlay {
                    // A call-activity child: its produced outputs (propagated
                    // child variables + the activity's already-applied output
                    // mappings) live in `overlay`, not the child scope. Lay them
                    // over the scope view and evaluate `output_element` against
                    // that — the element's `zeebe:output` was applied by the
                    // caller, so it is not re-applied here.
                    Some(overlay) => {
                        let mut vars = (*visible).clone();
                        vars.extend(overlay.clone());
                        crate::feel::eval(expr, &vars).ok()
                    }
                    None => {
                        let outputs = self.io_outputs(instance_key, &element_id);
                        if outputs.is_empty() {
                            crate::feel::eval(expr, &visible).ok()
                        } else {
                            match self.eval_io_mappings_in(&visible, &outputs) {
                                Ok(mapped) => {
                                    let mut vars = (*visible).clone();
                                    vars.extend(mapped);
                                    crate::feel::eval(expr, &vars).ok()
                                }
                                Err(failure) => {
                                    // An output mapping that fails to evaluate halts
                                    // the child with an incident instead of completing
                                    // it with a silently-unset output (#939). The child
                                    // does not complete; resolution re-drives it.
                                    let event = self.io_mapping_incident(
                                        instance_key,
                                        child_eik,
                                        element_id,
                                        failure,
                                        state::IoMappingRedrive::Completion,
                                    );
                                    return (vec![event], Vec::new());
                                }
                            }
                        }
                    }
                }
            }
        };
        events.push(Event::MultiInstanceChildCompleted {
            instance_key,
            body_key,
            child_key: child_eik,
            index,
            output,
        });

        // After each child, a satisfied completion condition ends the body early.
        let completion_now = completion_condition
            .as_deref()
            .map(|c| {
                matches!(
                    crate::feel::eval_bool(c, &self.variables(instance_key)),
                    Ok(true)
                )
            })
            .unwrap_or(false);

        let mut followups = Vec::new();
        // `active_now` still counts this child (its removal event above is not yet
        // applied), so the parallel body is drained when only this child remains.
        let others_active = active_now.saturating_sub(1);
        if completion_now {
            followups.push(Step::CompleteMiBody {
                instance_key,
                body_key,
            });
        } else if sequential {
            if spawned < total {
                followups.push(Step::ActivateMiChild {
                    instance_key,
                    element_id: mi_element,
                    body_key,
                    index: spawned,
                });
            } else {
                followups.push(Step::CompleteMiBody {
                    instance_key,
                    body_key,
                });
            }
        } else if others_active == 0 && spawned >= total {
            followups.push(Step::CompleteMiBody {
                instance_key,
                body_key,
            });
        }
        (events, followups)
    }

    /// Completes a multi-instance body: cancels any children still running (an
    /// early completion-condition fire), writes the aggregated output collection
    /// (padding uncollected slots with `null`) into the instance scope, applies
    /// the activity's output mappings, and takes its outgoing flow.
    fn complete_multi_instance_body(
        &mut self,
        instance_key: Key,
        body_key: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let (element_id, input_element, output_collection, output_values, active): (
            ElementId,
            Option<String>,
            Option<String>,
            Vec<Value>,
            Vec<Key>,
        ) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body_key))
        {
            Some(mi) => (
                mi.element_id.clone(),
                mi.input_element.clone(),
                mi.output_collection.clone(),
                mi.output_values
                    .iter()
                    .map(|o| o.clone().unwrap_or(Value::Null))
                    .collect(),
                mi.active.iter().copied().collect(),
            ),
            None => return (Vec::new(), Vec::new()),
        };
        let scope = self.scope_of(instance_key, body_key);

        // Build the aggregated output collection (padding uncollected slots with
        // null) and evaluate the activity's output mappings against the body-scope
        // view overlaid with that collection — all while the body scope is still
        // resident (its `ElementCompleted` below tears it down).
        let collection_map: Option<HashMap<String, Value>> =
            output_collection.map(|name| HashMap::from([(name, Value::List(output_values))]));
        let outputs = self.io_outputs(instance_key, &element_id);
        let output_updates = if outputs.is_empty() {
            HashMap::new()
        } else {
            let mut ctx = (*self.variables_for_element(instance_key, body_key)).clone();
            if let Some(map) = &collection_map {
                ctx.extend(map.clone());
            }
            // The body evaluates the ACTIVITY's own output mappings against the
            // body scope overlaid with the aggregated `outputCollection`. A mapping
            // that fails because it references a per-child binding
            // (`inputElement`/`loopCounter`) — absent at the body level — is
            // tolerated (skipped); it is authoritatively applied, and its eval
            // failure raised, per child in `complete_mi_child`. A mapping that fails
            // for any other reason is a genuine failure: park the body on an
            // `IO_MAPPING_ERROR` incident whose resolution re-drives this body
            // completion (#946), rather than silently completing with no incident.
            let mut tolerated = std::collections::HashSet::new();
            tolerated.insert("loopCounter".to_string());
            if let Some(name) = &input_element {
                tolerated.insert(name.clone());
            }
            match self.eval_io_mappings_tolerating(&ctx, &outputs, &tolerated) {
                Ok(updates) => updates,
                Err(failure) => {
                    let event = self.io_mapping_incident(
                        instance_key,
                        body_key,
                        element_id,
                        failure,
                        state::IoMappingRedrive::MiBodyCompletion,
                    );
                    return (vec![event], Vec::new());
                }
            }
        };

        let mut events = Vec::new();
        // Cancel any children still running (reached here via completion condition).
        // An ad-hoc container child (a JOB_WORKER ad-hoc MI child, #1170) owns its
        // OWN active tool scope (`adhoc_instances[child].active`), its agent job,
        // and an `adhoc_instances` runtime record — none of which the leaf-only
        // `cancel_mi_child_events` knows about. Tearing such a child down with the
        // leaf path alone would orphan its activated tools (open user tasks, tool
        // jobs, nested containers) and leave its ad-hoc record behind (no
        // `AdHocCompleted`), exactly as the interrupting-boundary teardown was
        // fixed to avoid (#1170). Route it through the container-aware
        // `cancel_adhoc_active_child` (recurses tools, emits `AdHocCompleted`)
        // instead, using the body as the enclosing container key; a non-ad-hoc
        // child (service task, sub-process, call activity) keeps the leaf path.
        for child in &active {
            if self.is_adhoc_container(instance_key, *child) {
                events.extend(self.cancel_adhoc_active_child(instance_key, body_key, *child));
            } else {
                // A non-ad-hoc MI child may itself be an embedded SUB-PROCESS that
                // opened its OWN token scope with an inner flow (inner jobs, timers,
                // nested scopes, open user tasks, call-activity children) running
                // under `child`. `cancel_mi_child_events` sweeps only resources
                // owned DIRECTLY by the child element instance, so tearing a
                // sub-process child down with the leaf path alone would leave its
                // inner job/scope active — orphaning the inner work and keeping the
                // parent process `Active` (the completion condition fires but the
                // instance never ends). Mirror the interrupting-boundary teardown:
                // sweep every descendant token first via `scope_teardown_events`
                // (which also resolves descendant incidents, resets parallel joins,
                // drops nested MI/ad-hoc records, terminates call-activity children
                // and clears scoped compensation) — a leaf child has no
                // descendants, so it returns nothing and the leaf behaviour is
                // byte-identical — then mark the scope torn down for this drain so a
                // still-queued inner activation cannot recreate a token inside the
                // dead scope (the per-drain dead-scope guard), and finally complete
                // the child element instance itself (#1170).
                events.extend(self.scope_teardown_events(instance_key, *child));
                self.torn_down_scopes.insert(*child);
                events.extend(self.cancel_mi_child_events(instance_key, *child));
            }
        }
        // Disarm every boundary event armed on the BODY as the loop enters
        // completion (before end listeners, mirroring the sub-process/ad-hoc
        // completion paths). An interrupting boundary on an MI activity is armed
        // on the body (`run_mi_body_activation`), so a natural or zero-item
        // completion must cancel those timers/subscriptions too — otherwise a
        // later timer/message could re-enter the boundary flow of an
        // already-completed body (#1170). A body with no attached boundaries
        // yields nothing here (the common case).
        events.extend(self.cancel_boundary_timers_on(body_key));
        events.extend(self.cancel_boundary_message_subscriptions_on(body_key));
        events.extend(self.cancel_boundary_signal_subscriptions_on(body_key));
        events.extend(self.cancel_boundary_conditional_subscriptions_on(body_key));
        // The output collection propagates OUT of the body to its enclosing (flow)
        // scope — for a top-level loop that is the root, collapsing to the flat
        // `VariablesUpdated`, byte-identical to the pre-scoping engine.
        if let Some(map) = &collection_map {
            events.extend(self.propagated_updates(instance_key, scope, map.clone(), false));
        }
        events.push(Event::ElementCompleting {
            instance_key,
            element_instance_key: body_key,
            element_id: element_id.clone(),
        });

        // End-listener gate (ADR 0037): the multi-instance body rests in COMPLETING
        // once every child has finished (Zeebe fires the activity's `end` listeners
        // at the body boundary, not per child). Its output mappings run first
        // (Zeebe ordering: mappings before listeners); `finalize_multi_instance_body`
        // emits the deferred `ElementCompleted` + `MultiInstanceCompleted` + outgoing
        // flows once the chain drains.
        if !self
            .listeners_of(
                instance_key,
                &element_id,
                crate::model::ListenerEventType::End,
            )
            .is_empty()
        {
            let mut listener_vars = (*self.variables_for_element(instance_key, body_key)).clone();
            if let Some(map) = &collection_map {
                listener_vars.extend(map.clone());
            }
            // Zeebe runs `end` listeners after output mappings, so the first
            // listener sees the mapped values — consistent with subsequent
            // listeners, which re-read the scope after the propagation below.
            listener_vars.extend(output_updates.clone());
            if let Some(job) = self.begin_end_listener_chain(
                instance_key,
                body_key,
                &element_id,
                scope,
                &listener_vars,
            ) {
                if !output_updates.is_empty() {
                    events.extend(self.propagated_updates(
                        instance_key,
                        scope,
                        output_updates,
                        false,
                    ));
                }
                events.push(job);
                return (events, Vec::new());
            }
        }

        events.push(Event::ElementCompleted {
            instance_key,
            element_instance_key: body_key,
            element_id: element_id.clone(),
        });
        events.push(Event::MultiInstanceCompleted {
            instance_key,
            body_key,
        });
        // Output mappings likewise propagate their projected result to the parent
        // (flow) scope.
        if !output_updates.is_empty() {
            events.extend(self.propagated_updates(instance_key, scope, output_updates, false));
        }
        let mut followups = Vec::new();
        for (taken, step) in self.take_all_flows(instance_key, &element_id, scope) {
            events.extend(taken);
            followups.push(step);
        }
        (events, followups)
    }

    /// Deferred completion of a multi-instance body whose `end` execution-listener
    /// chain has drained (ADR 0037). Its output collection + mappings already
    /// propagated when the body parked; this emits the parked `ElementCompleted` +
    /// `MultiInstanceCompleted` and takes the activity's outgoing flow, re-derived
    /// from the still-resident body record.
    fn finalize_multi_instance_body(
        &mut self,
        instance_key: Key,
        body_key: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let element_id = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.multi_instances.get(&body_key))
        {
            Some(mi) => mi.element_id.clone(),
            None => return (Vec::new(), Vec::new()),
        };
        let scope = self.scope_of(instance_key, body_key);
        let mut events = vec![
            Event::ElementCompleted {
                instance_key,
                element_instance_key: body_key,
                element_id: element_id.clone(),
            },
            Event::MultiInstanceCompleted {
                instance_key,
                body_key,
            },
        ];
        let mut followups = Vec::new();
        for (taken, step) in self.take_all_flows(instance_key, &element_id, scope) {
            events.extend(taken);
            followups.push(step);
        }
        (events, followups)
    }

    /// Builds the events that cancel a still-running multi-instance child (its job
    /// and element instance), for an early body completion. Returns events (it
    /// does not emit) so it composes inside a `process_step` result.
    fn cancel_mi_child_events(&self, instance_key: Key, child_eik: Key) -> Vec<Event> {
        let mut events = Vec::new();
        let element_id = self
            .element_id_of_instance(instance_key, child_eik)
            .unwrap_or_default();
        // A call-activity MI child parks its token on a distinct callee PROCESS
        // INSTANCE. The `cancel_all_*_on(child_eik)` sweeps below only reach
        // resources owned by this child element instance, never the separate
        // callee, so an early body completion (satisfied completion condition)
        // would leave the callee running with nothing left to complete it.
        // Terminate the callee here; its `ProcessInstanceTerminated` also seeds
        // the command tail's `cascade_cancel_children` to reap any transitive
        // grandchildren (#1170).
        if let Some(callee) = self.call_activity_child_of(child_eik) {
            events.extend(self.discard_instance_events(callee));
        }
        if let Some(job_key) = self.active_job_on(child_eik) {
            events.push(Event::JobCanceled {
                job_key,
                instance_key,
            });
        }
        events.extend(self.cancel_all_timers_on(child_eik));
        events.extend(self.cancel_all_subscriptions_on(child_eik));
        // A human task parked on this element instance must be explicitly
        // cancelled: completing the element instance alone leaves its
        // `state.user_tasks` entry in `Created` (see `state::apply` — only
        // `UserTaskCanceled` moves it to `Canceled`; `ElementCompleted` does
        // not touch it), orphaning the task. This surfaces when an embedded
        // subProcess tool body holds an open user task while its container is
        // cancel-remaining'd (#872). A leaf (non-user-task) child has no such
        // entry, so this is a no-op there.
        if let Some(user_task_key) = self
            .state
            .user_tasks
            .values()
            .find(|t| {
                t.instance_key == instance_key
                    && t.element_instance_key == child_eik
                    && t.state == state::UserTaskState::Created
            })
            .map(|t| t.key)
        {
            events.push(Event::UserTaskCanceled {
                user_task_key,
                instance_key,
            });
        }
        // Resolve any incident parked on the child ROOT itself before completing
        // it. `scope_teardown_events` resolves incidents for every *descendant* it
        // sweeps (via `resolve_incidents_on`) but never for the scope root, and
        // `ElementCompleted` touches no incident state — so a child parked on a
        // root incident (a service task's `JobNoRetries`, an input/output ioMapping
        // failure) would keep the completed instance carrying a stale `hasIncident`
        // for a vanished element, and a later external resolve could re-drive the
        // dead token (#1170). `ProcessInstanceCompleted` deliberately retains
        // incidents, so an early MI completion that clears the child's MI record
        // must close the child's own incident here. The `JobCanceled` above makes
        // any parked job terminal first, so the `IncidentResolved` reducer's
        // `Failed`-only job-resurrection guard leaves the cancelled job terminal.
        // A leaf child with no incident yields nothing here, so the behaviour is
        // byte-identical in the common case — mirrors the interrupting-boundary
        // teardown's body-root incident resolution.
        events.extend(self.resolve_incidents_on(instance_key, child_eik));
        events.push(Event::ElementCompleting {
            instance_key,
            element_instance_key: child_eik,
            element_id: element_id.clone(),
        });
        events.push(Event::ElementCompleted {
            instance_key,
            element_instance_key: child_eik,
            element_id,
        });
        events
    }

    /// Tear down one active ad-hoc tool child during a cancel-remaining-instances
    /// completion of its container, including the dedicated inner instance the
    /// tool hangs off. A plain (leaf) tool is cancelled via
    /// [`Self::cancel_mi_child_events`]; a NESTED ad-hoc container tool
    /// (agent-of-agents, #631) also owns its OWN active descendants (second-level
    /// tools/jobs), an agent job, and an `adhoc_instances` runtime entry — none of
    /// which `cancel_mi_child_events` knows about. Cancelling such a child with
    /// the leaf path alone would orphan the nested container's element
    /// instances/jobs and leave its ad-hoc state behind (no `AdHocCompleted`). So
    /// a nested container is torn down RECURSIVELY: its descendants are cancelled
    /// (each via this same routine), its own agent job / timers / subscriptions
    /// are disarmed, the container element instance is completed, and its ad-hoc
    /// runtime state is dropped via `AdHocCompleted { cancelled: true }`. Returns
    /// events (does not emit) so it composes inside a `process_step` result.
    fn cancel_adhoc_active_child(
        &self,
        instance_key: Key,
        container_key: Key,
        child: Key,
    ) -> Vec<Event> {
        let mut events = Vec::new();
        // Is this active child itself a nested ad-hoc container (#631)? If so it
        // has its own runtime scope + active descendants to tear down first.
        let nested = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&child))
            .map(|a| {
                (
                    a.element_id.clone(),
                    a.active.iter().copied().collect::<Vec<Key>>(),
                )
            });
        if let Some((nested_element_id, nested_active)) = nested {
            // Recursively cancel the nested container's own active tools (which
            // may themselves be nested containers), then disarm the nested
            // container's own agent job and any boundary state, complete its
            // element instance, and drop its ad-hoc runtime state.
            for grandchild in &nested_active {
                events.extend(self.cancel_adhoc_active_child(instance_key, child, *grandchild));
            }
            if let Some(job_key) = self.active_job_on(child) {
                events.push(Event::JobCanceled {
                    job_key,
                    instance_key,
                });
            }
            events.extend(self.cancel_all_timers_on(child));
            events.extend(self.cancel_all_subscriptions_on(child));
            // Resolve any incident parked on the nested container ROOT itself
            // (a failed agent job's `JobNoRetries`, an ioMapping failure) before
            // completing it — same root-incident gap as `cancel_mi_child_events`:
            // `ElementCompleted` touches no incident state and
            // `ProcessInstanceCompleted` retains incidents, so an early MI
            // completion would otherwise leave a stale active incident on the
            // removed container that a later resolve could re-drive against a dead
            // token (#1170). The `JobCanceled` above makes the parked agent job
            // terminal first, so the reducer's `Failed`-only resurrection guard
            // keeps it cancelled. Yields nothing when the container has no
            // incident, so the behaviour is byte-identical in the common case.
            events.extend(self.resolve_incidents_on(instance_key, child));
            events.push(Event::ElementCompleting {
                instance_key,
                element_instance_key: child,
                element_id: nested_element_id.clone(),
            });
            events.push(Event::ElementCompleted {
                instance_key,
                element_instance_key: child,
                element_id: nested_element_id,
            });
            events.push(Event::AdHocCompleted {
                instance_key,
                container_key: child,
                cancelled: true,
            });
        } else {
            // A leaf tool (service/user task) has no body scope, so
            // `cancel_mi_child_events` alone tears it down. An embedded-subProcess
            // tool (#872) additionally owns its own body scope: cancel every
            // active body descendant (open user tasks, jobs, armed timers,
            // subscriptions, nested scopes) before completing the subProcess
            // element instance itself, so cancelling the container leaves nothing
            // dangling. `scope_descendants` is empty for a leaf tool, so this is a
            // no-op there and the leaf behaviour is byte-identical.
            for descendant in self.scope_descendants(instance_key, child) {
                events.extend(self.cancel_mi_child_events(instance_key, descendant));
            }
            events.extend(self.cancel_mi_child_events(instance_key, child));
        }
        // A `callActivity` tool (issue #1159) is a leaf in the ad-hoc scope tree
        // but drives a distinct CHILD PROCESS INSTANCE hanging off this tool
        // element (`parentElementInstanceKey == child`). Cancelling the tool
        // (a completion condition or `cancelRemainingInstances`) leaves the parent
        // process instance alive, so the generic post-command `cascade_cancel_children`
        // sweep — which only reaps children of a *terminated* instance — never
        // reaps it. Terminate the linked child here so the callee and its jobs do
        // not outlive the ad-hoc token; emitting `ProcessInstanceTerminated` also
        // seeds `cascade_cancel_children` to reap any of the callee's own
        // grandchildren transitively.
        if let Some(call_child) = self.call_activity_child_of(child) {
            events.extend(self.discard_instance_events(call_child));
        }
        // Each tool child hangs off a dedicated inner instance; tear it down too
        // so the read-model element-instance tree does not leak an orphan. Only
        // when the inner instance is still active does its element id resolve —
        // if it is already gone there is nothing to tear down, and emitting a
        // completion with an empty element_id would corrupt downstream element
        // aggregates (mirrors the defensive skip in ModifyInstance termination).
        let inner = self.scope_of(instance_key, child);
        if inner != 0 && inner != container_key {
            if let Some(inner_element_id) = self.element_id_of_instance(instance_key, inner) {
                events.push(Event::ElementCompleting {
                    instance_key,
                    element_instance_key: inner,
                    element_id: inner_element_id.clone(),
                });
                events.push(Event::ElementCompleted {
                    instance_key,
                    element_instance_key: inner,
                    element_id: inner_element_id,
                });
            }
        }
        events
    }

    /// The ad-hoc catalog entry for `element_id` in `instance_key`'s definition,
    /// if that element is an ad-hoc sub-process container. Cloned so callers can
    /// hold it across the `&mut self` event emission that follows. This is the
    /// marker that gates all ad-hoc runtime behaviour (ADR 0023 seam 2), so the
    /// container needs no bespoke `ElementKind` variant.
    fn adhoc_def_of(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Option<crate::model::AdHocSubProcessDef> {
        self.process_of_instance(instance_key)?
            .adhoc
            .iter()
            .find(|d| d.container_id == element_id)
            .cloned()
    }

    /// Teardown events for a tool's dedicated inner-instance wrapper (gap #9).
    /// Emits `ElementCompleting` + `ElementCompleted` for the wrapper ONLY when its
    /// element id still resolves. An inner instance that has already left `active`
    /// (but lingers via `scopes`) would otherwise complete with an empty
    /// `element_id`, which corrupts downstream element aggregates — the same
    /// failure mode guarded in `cancel_adhoc_active_child` and ModifyInstance
    /// termination (regression `nested_adhoc_cancel_child_skips_already_completed_inner_instance`).
    /// Returns an empty vec when the id is unresolvable — there is nothing to tear
    /// down. Single source of truth for both the leaf and mid-chain tool paths.
    fn adhoc_inner_instance_teardown(&self, instance_key: Key, inner_key: Key) -> Vec<Event> {
        match self.element_id_of_instance(instance_key, inner_key) {
            Some(inner_element_id) => vec![
                Event::ElementCompleting {
                    instance_key,
                    element_instance_key: inner_key,
                    element_id: inner_element_id.clone(),
                },
                Event::ElementCompleted {
                    instance_key,
                    element_instance_key: inner_key,
                    element_id: inner_element_id,
                },
            ],
            None => Vec::new(),
        }
    }

    /// The follow-up sibling elements a just-completed ad-hoc tool chains into
    /// (issue #1154): the target of each of its outgoing `inner_flows` whose
    /// guard condition is absent or evaluates true in the tool's completed
    /// scope. Camunda drives these "structured sequence" flows out-of-band — the
    /// inner elements are pruned from the executable graph — so the container's
    /// catalog is the source of truth, not the flat flow graph. Returns the
    /// targets in document order; an empty result means the tool is a leaf of its
    /// execution path (the container's agent job re-emits once the path drains).
    fn adhoc_inner_flow_targets(
        &self,
        instance_key: Key,
        container_element_id: &str,
        tool_element_id: &str,
        child_eik: Key,
    ) -> Vec<String> {
        // Borrow the container's catalog by reference (no clone of the def or its
        // tool catalog) and stream-filter its `inner_flows` — this runs on every
        // tool completion, so it stays off the hot path's allocator.
        let Some(def) = self.process_of_instance(instance_key).and_then(|p| {
            p.adhoc
                .iter()
                .find(|d| d.container_id == container_element_id)
        }) else {
            return Vec::new();
        };
        // Conditions are evaluated against the source tool's completed scope
        // (`variables_for_element`), deliberately *not* the instance-level
        // variables an exclusive-gateway guard routes on: a structured-sequence
        // flow is guarded by what the emitting tool produced. A condition that
        // fails to evaluate (or is non-boolean) is treated as not taken,
        // mirroring the conservative "don't route on a broken guard" stance.
        let vars = self.variables_for_element(instance_key, child_eik);
        def.inner_flows
            .iter()
            .filter(|fl| fl.from == tool_element_id)
            .filter(|fl| match &fl.condition {
                None => true,
                Some(cond) => matches!(cond.eval(&vars), Ok(true)),
            })
            .map(|fl| fl.to.clone())
            .collect()
    }

    /// The advertised tool catalog an ad-hoc container writes to its local
    /// `adHocSubProcessElements` variable on activation (Camunda
    /// `AdHocSubProcessProcessor.onActivate`): one `{ elementId, elementName }`
    /// map per activatable tool in document order, excluding known
    /// non-activatable inner nodes (`AdHocToolKind::Other`, e.g. gateways) so the
    /// agent never sees an entry it cannot activate. Shared by the token-flow
    /// container activation (`run_activation_body`) and the nested-container tool
    /// activation (`activate_adhoc_tool`, #631) so both stand up an identical
    /// catalog.
    fn advertised_adhoc_catalog(def: &crate::model::AdHocSubProcessDef) -> Vec<Value> {
        def.tools
            .iter()
            .filter(|t| !matches!(t.kind, crate::model::AdHocToolKind::Other))
            .map(|t| {
                Value::Map(
                    [
                        ("elementId".to_string(), Value::Str(t.element_id.clone())),
                        ("elementName".to_string(), Value::Str(t.name.clone())),
                    ]
                    .into_iter()
                    .collect(),
                )
            })
            .collect()
    }

    /// Validate that every activate-element instruction names one of the
    /// container's tools (Zeebe NOT_FOUND parity — see
    /// `AdHocSubProcessInstructionActivateProcessor` /
    /// `JobCompleteProcessor.checkAdHocSubprocessActivationTargetsAreValid`).
    /// Shared by the agent-job completion path (#614 gap 4) and the external
    /// activate-activities command (#614 gap 3) so an unknown id is rejected
    /// identically on both seams — otherwise the loop would mint a phantom child
    /// that immediately completes. Atomic: the first unknown id fails the whole
    /// batch, before any activation applies.
    fn validate_adhoc_activation_targets(
        def: &crate::model::AdHocSubProcessDef,
        instance_key: Key,
        activate_elements: &[crate::model::AdHocActivateElement],
    ) -> Result<(), EngineError> {
        for instr in activate_elements {
            if !def.tools.iter().any(|t| t.element_id == instr.element_id) {
                return Err(EngineError::AdHocUnknownElement {
                    instance_key,
                    element_id: instr.element_id.clone(),
                });
            }
        }
        Ok(())
    }

    /// Enqueue the steps for one ad-hoc activation "turn" — shared by the
    /// agent-job completion path (#614 gap 4) and the external
    /// activate-activities command (#614 gap 3). Targets must already be
    /// validated (see [`Self::validate_adhoc_activation_targets`]). With
    /// `cancel_remaining`, cancels any in-flight tools and completes the
    /// container; otherwise activates each requested tool, and — when neither a
    /// tool is already active nor one is requested this turn — completes the
    /// container (nothing more to run).
    fn enqueue_adhoc_turn(
        &self,
        queue: &mut VecDeque<Step>,
        instance_key: Key,
        container_key: Key,
        activate_elements: Vec<crate::model::AdHocActivateElement>,
        cancel_remaining: bool,
    ) {
        if cancel_remaining {
            queue.push_back(Step::CompleteAdHoc {
                instance_key,
                container_key,
                cancel: true,
            });
            return;
        }
        let already_active = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
            .map(|a| a.active.len())
            .unwrap_or(0);
        let requested = activate_elements.len();
        for instr in activate_elements {
            queue.push_back(Step::ActivateAdHocTool {
                instance_key,
                container_key,
                element_id: instr.element_id,
                variables: instr.variables,
            });
        }
        // With no tool active and none requested, the agent has nothing more to
        // run this turn (it signals completion, or simply returns no
        // activations) — complete the container. Otherwise the container parks
        // until its tools drain, then its agent job is re-emitted for the next
        // turn.
        if already_active + requested == 0 {
            queue.push_back(Step::CompleteAdHoc {
                instance_key,
                container_key,
                cancel: false,
            });
        }
    }

    /// The `zeebe:ioMapping` of one tool inside an ad-hoc container, read from the
    /// container's catalog (the tool element is pruned from the executable graph,
    /// so `io_inputs`/`io_outputs` — which look it up by element id — return
    /// nothing for it). Empty when the tool declares no mappings (ADR 0023 seam 4).
    fn adhoc_tool_io(
        &self,
        instance_key: Key,
        container_element_id: &str,
        tool_element_id: &str,
    ) -> crate::model::IoMapping {
        // Borrow the container def rather than `adhoc_def_of`, which `.cloned()`s
        // the whole `AdHocSubProcessDef` (all `tools` + `inner_flows`). This runs
        // on every tool activation AND completion (including mid-chain), so clone
        // only the matched tool's `io`, never the entire catalog.
        self.process_of_instance(instance_key)
            .and_then(|p| {
                p.adhoc
                    .iter()
                    .find(|d| d.container_id == container_element_id)
            })
            .and_then(|def| def.tools.iter().find(|t| t.element_id == tool_element_id))
            .map(|t| t.io.clone())
            .unwrap_or_default()
    }

    /// Evaluates an ad-hoc tool's own `zeebe:ioMapping` INPUT mappings (sourced
    /// from the container catalog — the tool element is pruned from the flat
    /// graph) against `view`, the activating variable view. Returns the projected
    /// input updates (empty when the tool declares no inputs), or the evaluation
    /// failure so the caller can park the correct incident. Single source of truth
    /// for the tool-input projection shared by the tool's first activation
    /// (`activate_adhoc_tool`) and a call-activity tool's spawn retry
    /// (`respawn_adhoc_call_activity_tool`, issue #1159), so the two cannot drift.
    fn adhoc_tool_input_updates(
        &self,
        instance_key: Key,
        container_element_id: &str,
        tool_element_id: &str,
        view: &HashMap<String, Value>,
    ) -> Result<HashMap<String, Value>, IoMappingFailure> {
        let inputs = self
            .adhoc_tool_io(instance_key, container_element_id, tool_element_id)
            .inputs;
        if inputs.is_empty() {
            Ok(HashMap::new())
        } else {
            self.eval_io_mappings_in(view, &inputs)
        }
    }

    /// Evaluates a declarative ad-hoc container's `activeElementsCollection` FEEL
    /// expression to the ordered inner element ids to activate (Camunda BPMN_TASK
    /// variant; `AdHocSubProcessProcessor.readActivateElementsCollection`
    /// evaluates it as an array of strings). Only ids that exist in the
    /// container's tool catalog are kept: nano prunes inner tools from the
    /// executable graph, so an id absent from the catalog is not an activatable
    /// element — activating it would mint a phantom child. A non-list result, a
    /// non-string entry, or an unknown id is dropped here (v1.1 does not yet raise
    /// the Camunda `EXTRACT_VALUE_ERROR`/`NOT_FOUND` incident — tracked as the
    /// validation/rejection gaps).
    fn eval_adhoc_active_elements(
        &self,
        expr: &str,
        vars: &HashMap<String, Value>,
        def: &crate::model::AdHocSubProcessDef,
    ) -> Vec<String> {
        let names: std::collections::HashSet<&str> =
            def.tools.iter().map(|t| t.element_id.as_str()).collect();
        match crate::feel::eval(expr, vars) {
            Ok(Value::List(items)) => items
                .into_iter()
                .filter_map(|v| match v {
                    Value::Str(s) if names.contains(s.as_str()) => Some(s),
                    _ => None,
                })
                .collect(),
            _ => Vec::new(),
        }
    }

    /// Activates one ad-hoc "tool" child (ADR 0023 seam 2): instantiates
    /// `element_id` inside the container scope, seeding the agent's
    /// activate-element `variables` as the child's local overlay, and marks it
    /// active in the container. A service-task tool creates a job; a user-task
    /// tool parks a human task; an embedded-`subProcess` tool injects a token at
    /// its body's start event and runs the body by token flow (#872); any other
    /// kind passes straight through to completion (which feeds the loop).
    fn activate_adhoc_tool(
        &mut self,
        instance_key: Key,
        container_key: Key,
        element_id: String,
        variables: HashMap<String, Value>,
    ) -> (Vec<Event>, Vec<Step>) {
        let child_key = self.mint_key();
        // The scoped view the child evaluates its own FEEL attributes (job type,
        // retries, priority) against: the container scope overlaid with the
        // instruction's seed variables (applied via `AdHocToolActivated` below).
        let mut child_vars = (*self.variables_for_element(instance_key, container_key)).clone();
        child_vars.extend(variables.clone());

        // The container element id anchors both the tool's catalog lookups (job
        // type below, ioMapping here) and its scope.
        let container_element_id = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
            .map(|a| a.element_id.clone());

        // Tool input mappings (ADR 0023 seam 4): a tool's own `zeebe:ioMapping`
        // inputs are evaluated against the activating view (container scope +
        // agent seed variables) and folded into the child's local scope, exactly
        // like a leaf activity's inputs — but sourced from the container catalog
        // because the tool element is pruned from the executable graph.
        let mut local_variables = variables;
        // The tool's own input-mapping results, kept aside so a `callActivity`
        // tool can seed its spawned child with exactly them when
        // `propagateAllParentVariables=false` (issue #1159).
        let mut applied_inputs: HashMap<String, Value> = HashMap::new();
        if let Some(cid) = container_element_id.as_deref() {
            match self.adhoc_tool_input_updates(instance_key, cid, &element_id, &child_vars) {
                Ok(input_updates) => {
                    child_vars.extend(input_updates.clone());
                    local_variables.extend(input_updates.clone());
                    applied_inputs = input_updates;
                }
                Err(failure) => {
                    // A tool input mapping that fails to evaluate halts the
                    // ad-hoc container with an `IO_MAPPING_ERROR` incident
                    // rather than running the tool against a silently-unset
                    // variable (#939/#946). The tool child is not created; the
                    // container parks on the incident, and resolution re-drives
                    // the tool's *activation* (`AdHocToolActivation`) with its
                    // original seed variables — a clean retry, since nothing was
                    // created on this pass.
                    let event = self.io_mapping_incident(
                        instance_key,
                        container_key,
                        cid.to_string(),
                        failure,
                        state::IoMappingRedrive::AdHocToolActivation {
                            element_id: element_id.clone(),
                            variables: local_variables.clone(),
                        },
                    );
                    return (vec![event], Vec::new());
                }
            }
        }

        // Each tool runs beneath a dedicated `AD_HOC_SUB_PROCESS_INNER_INSTANCE`
        // element (Zeebe `BpmnAdHocSubProcessBehavior.createInnerInstance`): the
        // container's direct child in the element-instance tree is this inner
        // instance, and the tool child hangs off it. This is a read-model
        // (element-instance tree) nesting only — the inner instance opens no
        // variable scope, so the child's variable scope stays parented straight to
        // the container (`AdHocToolActivated` below), keeping variable resolution
        // and the activate-element loop byte-identical. The inner instance's id is
        // the container id with the `#innerInstance` postfix Zeebe uses.
        let inner_key = self.mint_key();
        let inner_element_id = container_element_id
            .as_deref()
            .map(adhoc_inner_instance_id)
            .unwrap_or_else(|| adhoc_inner_instance_id(&element_id));
        let mut events = vec![
            Event::ElementActivating {
                instance_key,
                element_instance_key: inner_key,
                element_id: inner_element_id.clone(),
            },
            Event::ElementActivated {
                instance_key,
                element_instance_key: inner_key,
                element_id: inner_element_id,
                scope: container_key,
            },
            Event::ElementActivating {
                instance_key,
                element_instance_key: child_key,
                element_id: element_id.clone(),
            },
            Event::ElementActivated {
                instance_key,
                element_instance_key: child_key,
                element_id: element_id.clone(),
                scope: inner_key,
            },
            Event::AdHocToolActivated {
                instance_key,
                container_key,
                child_key,
                local_variables,
            },
        ];
        let mut followups = Vec::new();
        // The tool's kind (and its job type / user-task props) comes from the
        // container's ad-hoc catalog, not the flat element graph: the parser
        // flattens an ad-hoc container to a single job activity and prunes its
        // inner tools, keeping each tool's id + kind in `ProcessDefinition.adhoc`
        // (so the executable element map — and the processos model round-trip —
        // stays identical to a plain container). A JOB_WORKER-style service-task
        // tool emits a job; a user-task tool creates a real user task and parks
        // the child until it is completed (ADR 0023 v1 scopes tools =
        // service/user tasks). Any other kind passes straight through to
        // completion (feeding the loop). v1 targets single-activity tools; the
        // catalog does not carry per-tool retries/priority for service tasks, so
        // those default (a later refinement).
        let tool_kind = container_element_id
            .as_deref()
            .and_then(|cid| self.adhoc_def_of(instance_key, cid))
            .and_then(|def| {
                def.tools
                    .iter()
                    .find(|t| t.element_id == element_id)
                    .map(|t| t.kind.clone())
            });

        // Nested ad-hoc sub-process tool (agent-of-agents, #631): the tool is
        // itself an `adHocSubProcess` — Zeebe's `isAdHocActivity` admits a nested
        // `AD_HOC_SUB_PROCESS` as an activatable tool. Instead of the opaque-job
        // fall-through below (which would run it as one leaf activity), stand it
        // up as a REAL second-level container: register its own ad-hoc runtime
        // scope, seed its own `outputCollection`, and either mint its own agent
        // job (JOB_WORKER) or run its declarative `activeElementsCollection`
        // (BPMN_TASK). The child element instance created above is the nested
        // container's scope; its own tools hang off it, so the read-model
        // element-instance tree nests container → inner instance → nested
        // container → its tools. Its completion propagates back through
        // `complete_adhoc_tool` of THIS container (see `complete_adhoc_container`),
        // feeding this container's `outputElement`/loop across the nesting
        // boundary.
        if let Some(nested_def) = self.adhoc_def_of(instance_key, &element_id) {
            events.push(Event::AdHocActivated {
                instance_key,
                container_key: child_key,
                element_id: element_id.clone(),
                output_collection: nested_def.output_collection.clone(),
                output_element: nested_def.output_element.clone(),
            });
            // Seed the nested container's `outputCollection` to an empty array as
            // a local variable, exactly as a top-level container does on
            // activation, so its agent can read the growing collection mid-run.
            if let Some(name) = nested_def.output_collection.clone() {
                events.push(Event::ScopedVariablesUpdated {
                    instance_key,
                    scope_key: child_key,
                    variables: HashMap::from([(name, Value::List(Vec::new()))]),
                });
            }
            if nested_def.impl_type == crate::model::AdHocImplementationType::BpmnTask {
                // Declarative nested container: activate the ids named by its
                // `activeElementsCollection` directly (no agent job), completing
                // once they drain.
                let ids = nested_def
                    .active_elements_collection
                    .as_deref()
                    .map(|expr| self.eval_adhoc_active_elements(expr, &child_vars, &nested_def))
                    .unwrap_or_default();
                for id in &ids {
                    followups.push(Step::ActivateAdHocTool {
                        instance_key,
                        container_key: child_key,
                        element_id: id.clone(),
                        variables: HashMap::new(),
                    });
                }
                if ids.is_empty() {
                    followups.push(Step::CompleteAdHoc {
                        instance_key,
                        container_key: child_key,
                        cancel: false,
                    });
                }
            } else {
                // Agentic JOB_WORKER nested container: advertise its own tool
                // catalog and mint its own agent job on the nested container
                // instance. Its job type is the nested container's resolved
                // `taskDefinition`, carried on this container's catalog as the
                // tool's `ServiceTask { job_type }` kind.
                events.push(Event::ScopedVariablesUpdated {
                    instance_key,
                    scope_key: child_key,
                    variables: HashMap::from([(
                        "adHocSubProcessElements".to_string(),
                        Value::List(Self::advertised_adhoc_catalog(&nested_def)),
                    )]),
                });
                let job_type = match &tool_kind {
                    Some(crate::model::AdHocToolKind::ServiceTask { job_type }) => job_type.clone(),
                    _ => element_id.clone(),
                };
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(&child_vars, &job_type);
                let priority = self.resolve_priority(&child_vars, None);
                let retries = self.resolve_retries(&child_vars, None);
                events.push(Event::JobCreated {
                    job_key,
                    instance_key,
                    element_instance_key: child_key,
                    element_id,
                    job_type,
                    created_at: self.now,
                    priority,
                    retries,
                });
            }
            return (events, followups);
        }

        match tool_kind {
            Some(crate::model::AdHocToolKind::ServiceTask { job_type }) => {
                let job_key = self.mint_key();
                let job_type = self.resolve_job_type(&child_vars, &job_type);
                let priority = self.resolve_priority(&child_vars, None);
                let retries = self.resolve_retries(&child_vars, None);
                events.push(Event::JobCreated {
                    job_key,
                    instance_key,
                    element_instance_key: child_key,
                    element_id,
                    job_type,
                    created_at: self.now,
                    priority,
                    retries,
                });
            }
            // A user-task tool parks the child on a real user task, resolving its
            // assignment/scheduling/priority expressions against the activating
            // view (container scope + seed vars + ioMapping inputs) exactly like
            // an ordinary user-task activation. It stays active until
            // `CompleteUserTask`, whose completion then routes through
            // `complete_adhoc_tool` (the child is in the container's active set),
            // feeding the container's `outputElement`/loop like any other tool.
            // Task listeners declared on the tool are pruned with it, so v1 emits
            // the plain CREATED record without a listener chain.
            Some(crate::model::AdHocToolKind::UserTask(props)) => {
                let user_task_key = self.mint_key();
                let assignee = self
                    .resolve_user_task_string(&child_vars, props.assignee.as_deref())
                    .filter(|s| !s.is_empty());
                let candidate_groups =
                    self.resolve_user_task_list(&child_vars, props.candidate_groups.as_deref());
                let candidate_users =
                    self.resolve_user_task_list(&child_vars, props.candidate_users.as_deref());
                let due_date = self
                    .resolve_user_task_string(&child_vars, props.due_date.as_deref())
                    .filter(|s| !s.is_empty());
                let follow_up_date = self
                    .resolve_user_task_string(&child_vars, props.follow_up_date.as_deref())
                    .filter(|s| !s.is_empty());
                let priority = self.resolve_priority(&child_vars, props.priority.as_deref());
                let (form_key, external_form_reference) =
                    self.resolve_user_task_form_linkage(&props);
                events.push(Event::UserTaskCreated {
                    user_task_key,
                    instance_key,
                    element_instance_key: child_key,
                    element_id,
                    created_at: self.now,
                    assignee,
                    candidate_groups,
                    candidate_users,
                    due_date,
                    follow_up_date,
                    priority,
                    form_key,
                    external_form_reference,
                });
            }
            // An embedded `subProcess` tool (#872): run its multi-element body by
            // ordinary token flow within the tool's scope. The tool child (the
            // subProcess element instance created above) is the body's variable
            // scope — already parented to the container by `AdHocToolActivated`,
            // so body elements resolve container + seed variables — and its token
            // scope for the inner flow. Inject a token at the body's start event
            // and park: the tool stays in the container's active set until the
            // body drains to its end event, at which point
            // `complete_drained_subprocesses` routes the drained tool through
            // `complete` → `complete_adhoc_tool` (append `outputElement`, tear
            // down the inner instance, re-emit the agent job). Boundary events on
            // the subProcess tool arm exactly like a normal embedded sub-process.
            Some(crate::model::AdHocToolKind::SubProcess { start_event }) => {
                events.extend(self.arm_boundary_events(
                    instance_key,
                    child_key,
                    inner_key,
                    &element_id,
                ));
                followups.push(Step::Activate {
                    instance_key,
                    element_id: start_event,
                    scope: child_key,
                    via: None,
                });
            }
            // A `callActivity` tool (issue #1159): spawn a distinct CHILD PROCESS
            // INSTANCE of its `calledElement`, exactly like a call activity on an
            // ordinary sequence flow — instead of the pass-through arm below,
            // which silently completed the tool child without ever instantiating
            // the callee (its output mapping then ran against variables the child
            // never set, manufacturing an all-null result). The tool child
            // element instance (`child_key`) is the call-activity token: it stays
            // ACTIVATED in the container's active set while the child runs, and
            // the child links back to it via `parentElementInstanceKey` so its
            // completion routes through `complete_call_activity` →
            // `complete_adhoc_call_activity_tool` (feeding the container's
            // `outputElement`/loop). The tool's input mappings were already
            // applied above (`applied_inputs`); `propagateAllParentVariables`
            // selects the child's seed — the full activating view, or only the
            // input results when suppressed.
            //
            // Only a BOUND callee (`process_id: Some`) spawns: the ad-hoc catalog
            // deliberately supports an UNBOUND call-activity tool (`processId`
            // absent → `process_id: None`, round-tripped as `UnboundCall` by
            // `processos`), which names no callee to instantiate. An unbound tool
            // falls through to the pass-through completion arm below (its
            // pre-#1159 behaviour) rather than being handed an empty callee that
            // would raise a spurious `CalledElementError`. A FEEL-expression
            // callee is `Some("=expr")`, so it still spawns and resolves at
            // dispatch — only a genuinely absent `calledElement` is `None`.
            Some(crate::model::AdHocToolKind::CallActivity {
                process_id: Some(called),
                propagate_all_parent_variables,
                ..
            }) => {
                // The actual first-pass child seed: the full activating view under
                // `propagateAllParentVariables` (Zeebe
                // `copyAllVariablesToProcessInstance`), or just the tool's
                // input-mapping results when suppressed
                // (`copyLocalVariablesToProcessInstance`).
                let child_seed = if propagate_all_parent_variables {
                    child_vars.clone()
                } else {
                    applied_inputs.clone()
                };
                // The redrive-preserved projection is ALWAYS only the single-pass
                // input-mapping results (#1176), never the frozen full view: on a
                // spawn-incident respawn we overlay it onto a FRESH all-parent view
                // (Zeebe re-reads the call activity's current scope in
                // `finalizeActivation`), so container variables that became visible
                // after the incident still propagate under
                // `propagateAllParentVariables=true`.
                let (spawn_events, spawn_followups) = self.spawn_call_activity_instance(
                    instance_key,
                    child_key,
                    &element_id,
                    &called,
                    &child_vars,
                    child_seed,
                    Some(applied_inputs),
                );
                events.extend(spawn_events);
                followups.extend(spawn_followups);
            }
            // Other / an unlisted id (including an UNBOUND `callActivity` tool with
            // no `calledElement`): no job, task, or callee to run, so the child
            // passes straight through to completion, feeding the loop.
            _ => {
                followups.push(Step::Complete {
                    instance_key,
                    element_instance_key: child_key,
                    element_id,
                });
            }
        }
        (events, followups)
    }

    /// Completes one ad-hoc tool child: records its output (the container's
    /// `outputElement` evaluated in the child's scope) into the container's
    /// accumulated results, drops it from the active set, and — once the last
    /// active tool of the turn completes — re-emits the container's agent job for
    /// the next activate-element turn (ADR 0023 seam 2).
    /// Evaluate an ad-hoc container's `<completionCondition>` (ADR 0023 seam 4)
    /// after one tool's output mappings have been projected into the container
    /// scope, and decide what the container does next. Shared by the leaf path
    /// (`complete_adhoc_tool`) and the mid-chain hand-off
    /// (`continue_adhoc_inner_flow`) so the completion semantics cannot drift
    /// between them.
    ///
    /// `output_updates` is the tool's just-applied output projection (overlaid on
    /// the container scope for the FEEL evaluation); `others` is the number of
    /// OTHER children still active (i.e. the active count excluding the tool that
    /// just completed). Zeebe latches a satisfied condition (`ElementInstance
    /// #isCompletionConditionFulfilled`), so a container that already deferred
    /// keeps completing on drain even if a later tool no longer satisfies it.
    fn adhoc_completion_after_tool(
        &self,
        instance_key: Key,
        container_key: Key,
        container_element_id: &str,
        output_updates: &HashMap<String, Value>,
        others: usize,
    ) -> AdHocPostTool {
        // Borrow the container's catalog entry once by reference (no clone of the
        // def or its `tools` / `inner_flows`) and reuse it for BOTH the
        // completion-condition evaluation and the `cancelRemainingInstances` read
        // — this runs on every tool completion, so it stays off the hot path's
        // allocator (mirrors `adhoc_inner_flow_targets`). A missing container has
        // no completion condition to honour, so the chain simply continues.
        let Some(def) = self.process_of_instance(instance_key).and_then(|p| {
            p.adhoc
                .iter()
                .find(|d| d.container_id == container_element_id)
        }) else {
            return AdHocPostTool::Continue;
        };
        let completion_now = def
            .completion_condition
            .as_ref()
            .map(|cond| {
                let mut ctx = (*self.variables_for_element(instance_key, container_key)).clone();
                ctx.extend(output_updates.iter().map(|(k, v)| (k.clone(), v.clone())));
                matches!(crate::feel::eval_bool(cond, &ctx), Ok(true))
            })
            .unwrap_or(false);
        let already_fulfilled = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
            .map(|a| a.completion_condition_fulfilled)
            .unwrap_or(false);
        if !(completion_now || already_fulfilled) {
            return AdHocPostTool::Continue;
        }
        let cancel_remaining = def.cancel_remaining_instances;
        if cancel_remaining {
            // `cancelRemainingInstances=true` (the BPMN default): complete now,
            // cancelling any tools still running this turn — exactly like a
            // multi-instance body's early completion.
            return AdHocPostTool::Complete { cancel: true };
        }
        // `cancelRemainingInstances=false`: defer container completion until no
        // active children/flows remain (Zeebe
        // `BpmnAdHocSubProcessBehavior#completionConditionFulfilled`). When the
        // last outstanding tool drains, complete without cancelling; otherwise
        // latch the fulfilment and park — no new agent turn, no further
        // activation — so the container completes as its children drain.
        if others == 0 {
            return AdHocPostTool::Complete { cancel: false };
        }
        let fulfilled_event = (!already_fulfilled).then(|| {
            Box::new(Event::AdHocCompletionConditionFulfilled {
                instance_key,
                container_key,
            })
        });
        AdHocPostTool::Defer { fulfilled_event }
    }

    fn complete_adhoc_tool(
        &mut self,
        instance_key: Key,
        child_eik: Key,
        element_id: String,
        container_key: Key,
        inner_key: Key,
        // The tool's output projection, ALREADY evaluated once against the real
        // completion scope (issue #1159): the call-activity tool bridge
        // (`complete_adhoc_call_activity_tool`) projects the output mapping against
        // the child process's produced variables and passes the result here so
        // this leaf does NOT re-evaluate it. Re-evaluating against the seeded child
        // scope would double-apply chained mappings (`status -> intermediate` then
        // `intermediate -> toolCallResult`), breaking `eval_io_mappings_in`'s
        // single-pass semantics where every mapping reads the original view. An
        // ordinary (single-activity) tool passes `None` and the mapping is
        // evaluated here as before.
        precomputed_output: Option<HashMap<String, Value>>,
    ) -> (Vec<Event>, Vec<Step>) {
        let tool_element_id = element_id;
        let (output_element, output_collection, active_now, container_element_id) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
        {
            Some(a) => (
                a.output_element.clone(),
                a.output_collection.clone(),
                a.active.len(),
                a.element_id.clone(),
            ),
            None => return (Vec::new(), Vec::new()),
        };
        // Issue #1154: an ad-hoc tool connected to a follow-up sibling by a
        // `bpmn:sequenceFlow` is NOT a leaf of its execution path — Camunda takes
        // that flow and runs the follow-up, re-emitting the container's agent job
        // only once the whole chain drains. So before treating this tool as a
        // path-completing leaf (append `outputElement`, evaluate the completion
        // condition, re-emit the agent job below), check for outgoing inner flows
        // and, when any are taken, continue the path instead.
        let chain_targets = self.adhoc_inner_flow_targets(
            instance_key,
            &container_element_id,
            &tool_element_id,
            child_eik,
        );
        if !chain_targets.is_empty() {
            return self.continue_adhoc_inner_flow(
                instance_key,
                child_eik,
                tool_element_id,
                container_key,
                inner_key,
                chain_targets,
                precomputed_output,
            );
        }
        // Collect this tool's output (evaluated in its local scope, which is still
        // resident — its `ElementCompleted` is deferred until the append is known
        // to be safe below) — an entry in the agent's accumulated
        // `outputCollection` memory.
        let output = output_element.as_deref().and_then(|expr| {
            let vars = self.variables_for_element(instance_key, child_eik);
            crate::feel::eval(expr, &vars).ok()
        });
        // Type guard (Zeebe `AdHocSubProcessOutputCollectionBehavior`): the
        // `outputCollection` target must be an array to append to. It is seeded to
        // `[]` on activation, so a non-array here means an input mapping (or the
        // agent) overwrote it with the wrong type. Read the container's *local*
        // binding directly (it is seeded/appended local to the container scope) —
        // no need to merge the whole visible variable chain.
        //
        // On a type mismatch, DEFER the tool's completion for full Zeebe parity
        // (retry-on-resolve): park an EXTRACT_VALUE_ERROR incident on the TOOL
        // child and return WITHOUT emitting any completion event. The child stays
        // in the container's `active` set with its local scope intact, so
        // resolving the incident re-drives `Step::Complete` for this child (see
        // `ResolveIncident` → `ExpressionEvaluation`) and re-attempts the append
        // once the target has been corrected — nothing is discarded.
        if output.is_some() {
            if let Some(name) = &output_collection {
                let current = self
                    .state
                    .instances
                    .get(&instance_key)
                    .and_then(|i| i.scope_variables.get(&container_key))
                    .and_then(|m| m.get(name))
                    .cloned();
                if matches!(current, Some(v) if !matches!(v, Value::List(_))) {
                    let incident_key = self.mint_key();
                    let reason = format!(
                        "the output collection '{name}' of ad-hoc sub-process \
                         '{container_element_id}' has the wrong type: expected an array"
                    );
                    // Preserve the tool's single-pass output projection on the
                    // redrive when it is a call-activity tool (#1176): the projection
                    // was already evaluated once by `complete_adhoc_call_activity_tool`
                    // against the child process's produced variables. A plain
                    // `Step::Complete` redrive would re-enter `complete_adhoc_tool`
                    // with `precomputed_output = None` and re-evaluate the tool's
                    // (possibly chained) output mappings against the seeded child
                    // scope — the double-eval PR #1171 fixed on the clean path.
                    // Carrying the projection lets the resolve reuse it verbatim. An
                    // ordinary (single-activity) tool has `None` here: its output
                    // re-evaluates idempotently against its own unmutated child scope,
                    // so it keeps the plain `redrive: None` → `Step::Complete` path.
                    let redrive = precomputed_output.clone().map(|precomputed_output| {
                        state::IoMappingRedrive::AdHocToolOutputCollection { precomputed_output }
                    });
                    return (
                        vec![Event::IncidentRaised {
                            incident_key,
                            instance_key,
                            element_instance_key: child_eik,
                            element_id: tool_element_id,
                            kind: state::IncidentKind::ExpressionEvaluation,
                            redrive,
                            reason,
                            job_key: None,
                            created_at: self.now,
                        }],
                        Vec::new(),
                    );
                }
            }
        }
        // The append is safe — complete the child now. `ElementCompleted` tears
        // its scope down when the caller applies these events, so any read of the
        // child scope (output mappings / completion condition below) must precede
        // that application (it does — events are batched, state is read live).
        //
        // The tool's dedicated inner instance (gap #9) completes WITH the child
        // (Zeebe `AdHocSubProcessInnerInstanceProcessor.afterExecutionPath
        // Completed`) so the read-model element-instance tree leaves nothing
        // dangling once the tool drains. This teardown is emitted here — after the
        // type guard — not at the top, so a deferred (incident-parked) tool keeps
        // its inner wrapper alive for the retry-on-resolve re-drive.
        let mut events = vec![
            Event::ElementCompleting {
                instance_key,
                element_instance_key: child_eik,
                element_id: tool_element_id.clone(),
            },
            Event::ElementCompleted {
                instance_key,
                element_instance_key: child_eik,
                element_id: tool_element_id.clone(),
            },
        ];
        events.extend(self.adhoc_inner_instance_teardown(instance_key, inner_key));
        events.push(Event::AdHocToolCompleted {
            instance_key,
            container_key,
            child_key: child_eik,
            output,
        });
        // Tool output mappings (ADR 0023 seam 4): project the tool's result into
        // the container scope, sourced from the container catalog (the pruned tool
        // has no element entry, so `io_outputs` cannot see it). Evaluated in the
        // child's local scope while it is still resident — its `ElementCompleted`
        // above tears the scope down only when the caller applies the events.
        let output_updates = match precomputed_output {
            // Call-activity tool bridge (#1159): the output mapping was already
            // projected ONCE against the child's real produced variables. Use it
            // verbatim — re-evaluating it here against the seeded child scope would
            // double-apply chained mappings.
            Some(updates) => updates,
            None => {
                let outputs = self
                    .adhoc_tool_io(instance_key, &container_element_id, &tool_element_id)
                    .outputs;
                if outputs.is_empty() {
                    HashMap::new()
                } else {
                    let vars = self.variables_for_element(instance_key, child_eik);
                    match self.eval_io_mappings_in(&vars, &outputs) {
                        Ok(updates) => updates,
                        Err(failure) => {
                            // A tool output mapping that fails to evaluate halts the
                            // tool with an incident instead of completing it with a
                            // silently-unset output (#939). The tool does not complete;
                            // resolution re-drives its completion.
                            let event = self.io_mapping_incident(
                                instance_key,
                                child_eik,
                                tool_element_id,
                                failure,
                                state::IoMappingRedrive::Completion,
                            );
                            return (vec![event], Vec::new());
                        }
                    }
                }
            }
        };
        if !output_updates.is_empty() {
            events.extend(self.propagated_updates(
                instance_key,
                container_key,
                output_updates.clone(),
                false,
            ));
        }
        // Completion condition (ADR 0023 seam 4): a declared `<completionCondition>`
        // is evaluated after each tool completes, against the container scope
        // overlaid with the output mappings just projected into it. `active_now`
        // still counts this child (its removal above is not yet applied), so the
        // last tool of the turn is the one leaving one active.
        let others = active_now.saturating_sub(1);
        match self.adhoc_completion_after_tool(
            instance_key,
            container_key,
            &container_element_id,
            &output_updates,
            others,
        ) {
            AdHocPostTool::Complete { cancel } => {
                return (
                    events,
                    vec![Step::CompleteAdHoc {
                        instance_key,
                        container_key,
                        cancel,
                    }],
                );
            }
            AdHocPostTool::Defer { fulfilled_event } => {
                if let Some(event) = fulfilled_event {
                    events.push(*event);
                }
                return (events, Vec::new());
            }
            AdHocPostTool::Continue => {}
        }
        if others == 0 {
            // The declarative (BPMN_TASK) variant activates its collection once
            // and completes when those elements drain — there is no agent to
            // re-emit a job for (ADR 0023 v1.1). The agentic (JOB_WORKER) variant
            // re-emits its job so the agent decides the next turn.
            let declarative = self
                .adhoc_def_of(instance_key, &container_element_id)
                .map(|d| d.impl_type == crate::model::AdHocImplementationType::BpmnTask)
                .unwrap_or(false);
            if declarative {
                return (
                    events,
                    vec![Step::CompleteAdHoc {
                        instance_key,
                        container_key,
                        cancel: false,
                    }],
                );
            }
            // Every tool this turn has drained: re-emit the agent job so the agent
            // inspects the accumulated results and decides the next turn (activate
            // more tools, or signal completion).
            events.push(Event::AdHocIterated {
                instance_key,
                container_key,
            });
            events.extend(self.adhoc_agent_job_events(
                instance_key,
                container_key,
                container_element_id,
            ));
        }
        (events, Vec::new())
    }

    /// Continues an ad-hoc "structured sequence" (issue #1154): a completing tool
    /// that has one or more taken outgoing inner flows is not a leaf of its
    /// execution path, so instead of completing the path (append `outputElement`,
    /// re-emit the agent job) this completes the tool + its inner instance,
    /// projects the tool's output mappings into the container scope so the
    /// follow-up can read them, drops the tool from the active set WITHOUT
    /// appending to `outputCollection` (the leaf appends the path's result), and
    /// activates each follow-up sibling as a fresh tool child of the container.
    /// The container's `<completionCondition>` is still evaluated on this
    /// hand-off (via [`Self::adhoc_completion_after_tool`], shared with the leaf
    /// path): if it triggers completion the sub-process ends now and the
    /// follow-up sibling(s) are NOT activated. Otherwise the container's agent
    /// job is NOT re-emitted mid-chain — the just-completed tool is dropped from
    /// the active set and its follow-up sibling(s) activated in its place, so the
    /// chain simply keeps draining. The agent job is re-emitted only once the
    /// whole chain drains to a leaf (a tool with no taken outgoing inner flow).
    #[allow(clippy::too_many_arguments)]
    fn continue_adhoc_inner_flow(
        &mut self,
        instance_key: Key,
        child_eik: Key,
        tool_element_id: String,
        container_key: Key,
        inner_key: Key,
        targets: Vec<String>,
        // The tool's output projection, ALREADY evaluated once against the real
        // completion scope (issue #1159) — carried through from
        // `complete_adhoc_tool` for a `callActivity` tool whose result was
        // projected by the bridge (`complete_adhoc_call_activity_tool`). A tool
        // that chains into a follow-up sibling is not a leaf, but its output
        // mappings must STILL project single-pass: re-evaluating them here against
        // the seeded child scope would double-apply chained mappings
        // (`summary -> summaryCopy` then `summaryCopy -> toolCallResult`), exactly
        // the divergence the leaf path guards against. `Some` ⇒ use verbatim;
        // an ordinary tool passes `None` and the mapping is evaluated here.
        precomputed_output: Option<HashMap<String, Value>>,
    ) -> (Vec<Event>, Vec<Step>) {
        // Resolve the container's identity and its active count from live state.
        // The caller already early-returns when this lookup is absent, but resolve
        // it safely here too: an empty `element_id` fallback would make
        // `adhoc_tool_io` / the completion condition read IO mappings under the
        // wrong container id and continue the chain with missing projections, so a
        // genuinely missing container returns early (no events) instead. `active`
        // still counts this completing tool (its removal is not yet applied).
        let (container_element_id, active_now) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
        {
            Some(a) => (a.element_id.clone(), a.active.len()),
            None => return (Vec::new(), Vec::new()),
        };
        let mut events = vec![
            Event::ElementCompleting {
                instance_key,
                element_instance_key: child_eik,
                element_id: tool_element_id.clone(),
            },
            Event::ElementCompleted {
                instance_key,
                element_instance_key: child_eik,
                element_id: tool_element_id.clone(),
            },
        ];
        // Tear down the tool's dedicated inner-instance wrapper (only when its id
        // resolves — see `adhoc_inner_instance_teardown`).
        events.extend(self.adhoc_inner_instance_teardown(instance_key, inner_key));
        events.push(
            // An intermediate node of a chained path is not a path completion, so
            // `output` is `None`: nothing is appended to `outputCollection` (the
            // leaf appends the path's result). This event drops the tool from the
            // container's active set.
            Event::AdHocToolCompleted {
                instance_key,
                container_key,
                child_key: child_eik,
                output: None,
            },
        );
        // Tool output mappings (ADR 0023 seam 4) still project the tool's result
        // into the container scope so the follow-up can read it — evaluated in the
        // child's local scope while it is still resident (its `ElementCompleted`
        // above tears the scope down only when the caller applies these events).
        let output_updates = match precomputed_output {
            // Call-activity tool bridge (#1159): the output mapping was already
            // projected ONCE against the child's real produced variables. Use it
            // verbatim — re-evaluating it here against the seeded child scope would
            // double-apply chained mappings, so a chained-flow hand-off must
            // preserve single-pass semantics exactly like the leaf path.
            Some(updates) => updates,
            None => {
                let outputs = self
                    .adhoc_tool_io(instance_key, &container_element_id, &tool_element_id)
                    .outputs;
                if outputs.is_empty() {
                    HashMap::new()
                } else {
                    let vars = self.variables_for_element(instance_key, child_eik);
                    match self.eval_io_mappings_in(&vars, &outputs) {
                        Ok(updates) => updates,
                        Err(failure) => {
                            // A failing tool output mapping halts the tool with an incident
                            // rather than continuing the chain with a silently-unset output
                            // (#939); resolution re-drives its completion, re-evaluating the
                            // chain.
                            let event = self.io_mapping_incident(
                                instance_key,
                                child_eik,
                                tool_element_id,
                                failure,
                                state::IoMappingRedrive::Completion,
                            );
                            return (vec![event], Vec::new());
                        }
                    }
                }
            }
        };
        if !output_updates.is_empty() {
            events.extend(self.propagated_updates(
                instance_key,
                container_key,
                output_updates.clone(),
                false,
            ));
        }
        // Completion condition (ADR 0023 seam 4) is evaluated after EVERY tool
        // completes — the mid-chain hand-off is no exception. If the container's
        // `<completionCondition>` (or a previously latched fulfilment) triggers
        // completion now, the sub-process is ending: short-circuit and complete it
        // rather than activating the follow-up sibling(s), which would spawn fresh
        // tools into a container that is already completing.
        let others = active_now.saturating_sub(1);
        match self.adhoc_completion_after_tool(
            instance_key,
            container_key,
            &container_element_id,
            &output_updates,
            others,
        ) {
            AdHocPostTool::Complete { cancel } => {
                return (
                    events,
                    vec![Step::CompleteAdHoc {
                        instance_key,
                        container_key,
                        cancel,
                    }],
                );
            }
            AdHocPostTool::Defer { fulfilled_event } => {
                if let Some(event) = fulfilled_event {
                    events.push(*event);
                }
                return (events, Vec::new());
            }
            AdHocPostTool::Continue => {}
        }
        // Take each outgoing inner flow and activate its target as a fresh tool
        // child of the container, continuing the execution path.
        let mut followups = Vec::new();
        for target in targets {
            events.push(Event::SequenceFlowTaken {
                instance_key,
                from: tool_element_id.clone(),
                to: target.clone(),
            });
            followups.push(Step::ActivateAdHocTool {
                instance_key,
                container_key,
                element_id: target,
                variables: HashMap::new(),
            });
        }
        (events, followups)
    }

    /// Builds the `JobCreated` event that (re-)emits an ad-hoc container's agent
    /// job on the container element instance, so the agent worker activates it to
    /// inspect accumulated tool results and drive the next turn.
    fn adhoc_agent_job_events(
        &mut self,
        instance_key: Key,
        container_key: Key,
        container_element_id: String,
    ) -> Vec<Event> {
        let job_type =
            match self.adhoc_container_job_type(instance_key, container_key, &container_element_id)
            {
                Some(job_type) => job_type,
                None => return Vec::new(),
            };
        let container_vars = self.variables_for_element(instance_key, container_key);
        let job_type = self.resolve_job_type(&container_vars, &job_type);
        let retries = self.resolve_retries(
            &container_vars,
            self.retries_of(instance_key, &container_element_id)
                .as_deref(),
        );
        let priority = self.resolve_priority(&container_vars, None);
        let job_key = self.mint_key();
        vec![Event::JobCreated {
            job_key,
            instance_key,
            element_instance_key: container_key,
            element_id: container_element_id,
            job_type,
            created_at: self.now,
            priority,
            retries,
        }]
    }

    /// Resolves the agent-job type of an ad-hoc container. A top-level container
    /// carries it on its executable `ServiceTask` element. A NESTED container
    /// (agent-of-agents, #631) is pruned from the executable graph, so its
    /// `taskDefinition` is not readable by element id; it is instead carried on
    /// the PARENT container's tool catalog as this tool's
    /// `AdHocToolKind::ServiceTask { job_type }`. Falls back to the catalog when
    /// the element graph has no entry.
    fn adhoc_container_job_type(
        &self,
        instance_key: Key,
        container_key: Key,
        container_element_id: &str,
    ) -> Option<String> {
        if let Some(ElementKind::ServiceTask { job_type, .. }) =
            self.element_kind(instance_key, container_element_id)
        {
            return Some(job_type);
        }
        // Nested container: walk the element-instance tree (inner instance →
        // parent container) and read this tool's job type off the parent catalog.
        let inner = self.scope_of(instance_key, container_key);
        let parent = if inner != 0 {
            self.scope_of(instance_key, inner)
        } else {
            0
        };
        let parent_element_id = self.element_id_of_instance(instance_key, parent)?;
        self.adhoc_def_of(instance_key, &parent_element_id)?
            .tools
            .iter()
            .find(|t| t.element_id == container_element_id)
            .and_then(|t| match &t.kind {
                crate::model::AdHocToolKind::ServiceTask { job_type } => Some(job_type.clone()),
                _ => None,
            })
    }

    /// Completes an ad-hoc container (ADR 0023 seam 2): when `cancel`, cancels any
    /// tool children still running; writes the aggregated `outputCollection` into
    /// the enclosing (flow) scope; drops the container's runtime state; and takes
    /// the container's outgoing flow so the parent token continues.
    fn complete_adhoc_container(
        &mut self,
        instance_key: Key,
        container_key: Key,
        cancel: bool,
    ) -> (Vec<Event>, Vec<Step>) {
        let (element_id, output_collection, active) = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
        {
            Some(a) => (
                a.element_id.clone(),
                a.output_collection.clone(),
                a.active.iter().copied().collect::<Vec<Key>>(),
            ),
            None => return (Vec::new(), Vec::new()),
        };
        let scope = self.scope_of(instance_key, container_key);

        // A JOB_WORKER ad-hoc container that is a MULTI-INSTANCE child (its token
        // parks in an MI body whose loop element is this container): its
        // completion must feed the loop's output collection / completion condition
        // and advance the loop, NOT take the activity's outgoing flow. This path
        // is reached directly when the container's agent turn completes, bypassing
        // the MI-child detection in `complete`, so without this route a
        // normally-completing MI ad-hoc loop leaves the container's key in the
        // body's `active` set and hangs forever (#1170). The container's own
        // `outputCollection` variable (seeded on activation, appended per tool)
        // lives in the container/child scope, so `complete_mi_child` reads it via
        // the child scope view for the MI `output_element`.
        let is_mi_child = scope != 0
            && self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.multi_instances.get(&scope))
                .map(|mi| mi.element_id == element_id)
                .unwrap_or(false);
        if is_mi_child {
            let mut events = Vec::new();
            // A `cancel`-remaining-instances completion still tears the container's
            // own live tools down (each hangs off a dedicated inner instance)
            // before the child completes; a natural completion has none active.
            if cancel {
                for child in &active {
                    events.extend(self.cancel_adhoc_active_child(
                        instance_key,
                        container_key,
                        *child,
                    ));
                }
            }
            // Drop the container's ad-hoc runtime record; its `outputCollection`
            // stays resident in the child scope for the MI `output_element` below.
            events.push(Event::AdHocCompleted {
                instance_key,
                container_key,
                cancelled: cancel,
            });
            let (mi_events, mi_followups) =
                self.complete_mi_child(instance_key, container_key, element_id, scope, None);
            events.extend(mi_events);
            return (events, mi_followups);
        }

        // Detect up front whether this container is itself an active tool of a
        // PARENT ad-hoc container (nested agent-of-agents, #631). Detected exactly
        // like the tool-completion routing in `complete`: walk the
        // element-instance tree (inner instance → parent container) and confirm
        // the parent still lists this container active. `scope` is this
        // container's inner-instance wrapper, so it doubles as `inner_key`.
        let inner_key = scope;
        let parent_container = if inner_key != 0 {
            self.scope_of(instance_key, inner_key)
        } else {
            0
        };
        let is_nested_tool = parent_container != 0
            && self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.adhoc_instances.get(&parent_container))
                .map(|a| a.active.contains(&container_key))
                .unwrap_or(false);

        let mut events = Vec::new();
        // Cancel any tools still running (a cancel-remaining-instances request).
        // Each active tool child hangs off a dedicated inner instance, so tearing
        // the child down also tears down its inner instance — otherwise the
        // read-model element-instance tree would leak an orphaned inner instance.
        // A nested ad-hoc container tool (#631) is torn down recursively (its own
        // descendants + agent job + ad-hoc state), which `cancel_adhoc_active_child`
        // handles — cancelling it as a leaf would orphan its internals.
        if cancel {
            for child in &active {
                events.extend(self.cancel_adhoc_active_child(instance_key, container_key, *child));
            }
        }
        // The output collection propagates OUT of the container to its enclosing
        // (flow) scope — for a top-level container that is the root, collapsing to
        // the flat `VariablesUpdated`. Its value is read from the container-scope
        // variable directly (the single source of truth, seeded on activation and
        // appended to as each tool completed), and propagated AS-IS: with the
        // retry-on-resolve type guard in `complete_adhoc_tool`, a non-array here
        // can only be a value the guard deliberately preserved, so coercing it to
        // `[]` would silently corrupt it. A never-seeded collection defaults to an
        // empty array.
        //
        // A NESTED container (agent-of-agents, #631) is the exception: its result
        // must cross the nesting boundary ONLY via the parent's tool-completion
        // path below (projected through the PARENT's `outputElement`). Propagating
        // its own `outputCollection` (e.g. `subResults`) into the enclosing parent
        // scope here would both leak the nested container's internal collection
        // into the parent's variables and apply that update even when the boundary
        // crossing is later DEFERRED (incident) by `complete_adhoc_tool`. So skip
        // this propagation entirely when nested.
        let collection_map = output_collection.map(|name| {
            let value = self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.scope_variables.get(&container_key))
                .and_then(|m| m.get(&name))
                .cloned()
                .unwrap_or_else(|| Value::List(Vec::new()));
            HashMap::from([(name, value)])
        });
        if let Some(map) = &collection_map {
            if !is_nested_tool {
                events.extend(self.propagated_updates(instance_key, scope, map.clone(), false));
            }
        }

        // Nested ad-hoc container completing (agent-of-agents, #631): this
        // container is itself an active tool of a parent ad-hoc container. Rather
        // than take a (pruned, non-existent) outgoing flow, its completion crosses
        // the nesting boundary through the parent's `complete_adhoc_tool` — the
        // same path an ordinary tool completion takes — which projects this
        // container's result via the PARENT's `outputElement`, drops it from the
        // parent's active set (tearing down its wrapping inner instance), and
        // re-emits the parent's agent job once the parent's tools drain. The
        // `is_nested_tool` detection was computed up front (above), so its own
        // `outputCollection` was already withheld from the enclosing scope. The
        // end-listener gate below is intentionally skipped for a nested container
        // — its completion is a tool completion, not a token-flow container
        // completion.
        if is_nested_tool {
            let (tool_events, tool_followups) = self.complete_adhoc_tool(
                instance_key,
                container_key,
                element_id.clone(),
                parent_container,
                inner_key,
                None,
            );
            // `complete_adhoc_tool` DEFERS (raising an incident, no
            // `AdHocToolCompleted`) if the parent's outputCollection is
            // mis-typed; in that case leave this container's runtime state intact
            // so resolving the incident re-drives the completion — mirroring the
            // tool-level retry-on-resolve. Only tear the nested container's
            // ad-hoc state down once the boundary crossing actually completed.
            let completed = tool_events
                .iter()
                .any(|e| matches!(e, Event::AdHocToolCompleted { .. }));
            events.extend(tool_events);
            if completed {
                events.push(Event::AdHocCompleted {
                    instance_key,
                    container_key,
                    cancelled: cancel,
                });
            }
            return (events, tool_followups);
        }

        events.push(Event::ElementCompleting {
            instance_key,
            element_instance_key: container_key,
            element_id: element_id.clone(),
        });

        // End-listener gate (ADR 0037): fires the container's `end` listeners at
        // the container boundary once its tools have drained.
        // `finalize_adhoc_container` emits the parked `ElementCompleted` +
        // `AdHocCompleted` + outgoing flows. Only the natural completion is gated;
        // a cancel-remaining-instances completion (`cancel`) stays inline — its
        // `cancelled` flag cannot be re-derived at drain time, and running end
        // listeners on an aborted container is not meaningful.
        if !cancel
            && !self
                .listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::ListenerEventType::End,
                )
                .is_empty()
        {
            let mut listener_vars =
                (*self.variables_for_element(instance_key, container_key)).clone();
            if let Some(map) = &collection_map {
                listener_vars.extend(map.clone());
            }
            if let Some(job) = self.begin_end_listener_chain(
                instance_key,
                container_key,
                &element_id,
                scope,
                &listener_vars,
            ) {
                // Disarm any boundary events so none can fire during the
                // COMPLETING window the end chain opens (defensive/symmetric with
                // the sub-process path; a no-op when the container carries none).
                events.extend(self.cancel_boundary_timers_on(container_key));
                events.extend(self.cancel_boundary_message_subscriptions_on(container_key));
                events.extend(self.cancel_boundary_signal_subscriptions_on(container_key));
                events.extend(self.cancel_boundary_conditional_subscriptions_on(container_key));
                events.push(job);
                return (events, Vec::new());
            }
        }

        events.push(Event::ElementCompleted {
            instance_key,
            element_instance_key: container_key,
            element_id: element_id.clone(),
        });
        events.push(Event::AdHocCompleted {
            instance_key,
            container_key,
            cancelled: cancel,
        });
        let mut followups = Vec::new();
        for (taken, step) in self.take_all_flows(instance_key, &element_id, scope) {
            events.extend(taken);
            followups.push(step);
        }
        (events, followups)
    }

    /// Deferred completion of an ad-hoc sub-process container whose `end`
    /// execution-listener chain has drained (ADR 0037). Its output collection
    /// already propagated when the container parked; this emits the parked
    /// `ElementCompleted` + `AdHocCompleted` and takes the outgoing flow.
    fn finalize_adhoc_container(
        &mut self,
        instance_key: Key,
        container_key: Key,
        cancelled: bool,
    ) -> (Vec<Event>, Vec<Step>) {
        let element_id = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
        {
            Some(a) => a.element_id.clone(),
            None => return (Vec::new(), Vec::new()),
        };
        let scope = self.scope_of(instance_key, container_key);
        let mut events = vec![
            Event::ElementCompleted {
                instance_key,
                element_instance_key: container_key,
                element_id: element_id.clone(),
            },
            Event::AdHocCompleted {
                instance_key,
                container_key,
                cancelled,
            },
        ];
        let mut followups = Vec::new();
        for (taken, step) in self.take_all_flows(instance_key, &element_id, scope) {
            events.extend(taken);
            followups.push(step);
        }
        (events, followups)
    }

    fn complete(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        // A completing element instance whose scope is a multi-instance body is
        // one of that body's children: its completion feeds the loop's output
        // collection and completion condition rather than taking the activity's
        // outgoing flow directly.
        let scope = self.scope_of(instance_key, element_instance_key);
        if scope != 0
            && self
                .state
                .instances
                .get(&instance_key)
                .and_then(|i| i.multi_instances.get(&scope))
                .map(|mi| mi.element_id == element_id)
                .unwrap_or(false)
        {
            return self.complete_mi_child(
                instance_key,
                element_instance_key,
                element_id,
                scope,
                None,
            );
        }

        // A completing element instance that is an ad-hoc tool: it hangs off an
        // `AD_HOC_SUB_PROCESS_INNER_INSTANCE` whose own scope is the ad-hoc
        // container that still lists the tool child active. Its completion feeds
        // the container's output collection and the activate-element loop rather
        // than taking an outgoing flow, and tears the inner instance down with it.
        // This covers both single-activity tools (service/user tasks) and an
        // embedded-`subProcess` tool whose multi-element body has drained to its
        // end event (#872) — the latter reaches here via
        // `complete_drained_subprocesses` routing the drained tool to
        // `Step::Complete`.
        if scope != 0 {
            let container = self.scope_of(instance_key, scope);
            if container != 0
                && self
                    .state
                    .instances
                    .get(&instance_key)
                    .and_then(|i| i.adhoc_instances.get(&container))
                    .map(|a| a.active.contains(&element_instance_key))
                    .unwrap_or(false)
            {
                return self.complete_adhoc_tool(
                    instance_key,
                    element_instance_key,
                    element_id,
                    container,
                    scope,
                    None,
                );
            }
        }

        if matches!(
            self.element_kind(instance_key, &element_id),
            Some(ElementKind::ExclusiveGateway)
        ) {
            return self.complete_exclusive_gateway(instance_key, element_instance_key, element_id);
        }

        // An inclusive gateway reaching completion is a pure split (a join is
        // intercepted at `activate` and routed by `activate_join`).
        // It routes every outgoing flow whose condition holds (else the default).
        if matches!(
            self.element_kind(instance_key, &element_id),
            Some(ElementKind::InclusiveGateway)
        ) {
            return self.complete_inclusive_gateway(instance_key, element_instance_key, element_id);
        }

        // A terminate end event (`<endEvent>` with `<terminateEventDefinition>`):
        // it does not merely consume its own token — it kills every other active
        // token in its enclosing scope and completes that scope (Zeebe/Camunda
        // terminate semantics). Handled wholly here, off the normal end-event
        // completion path, because it drives a scope-wide teardown rather than
        // taking (non-existent) outgoing flows.
        if matches!(
            self.element_kind(instance_key, &element_id),
            Some(ElementKind::TerminateEndEvent)
        ) {
            return self.complete_terminate_end(
                instance_key,
                element_instance_key,
                element_id,
                scope,
            );
        }

        // Inline-FEEL script task: evaluate its `zeebe:script` expression now.
        // The instance variables already include any input mappings applied when
        // the task activated. On success the result is staged under
        // `resultVariable` and written before output mappings run (so a
        // `zeebe:output` can reference it); on failure an ExpressionEvaluation
        // incident is raised and the element stays active — matching Zeebe, which
        // re-evaluates the script when the incident is resolved (the resolution
        // re-runs completion), exactly as an exclusive gateway condition does.
        let mut script_update: Option<HashMap<String, Value>> = None;
        if let Some(ElementKind::ScriptTask {
            expression,
            result_variable,
        }) = self.element_kind(instance_key, &element_id)
        {
            let vars = self.variables(instance_key);
            match crate::feel::eval(&expression, &vars) {
                Ok(value) => {
                    let mut update = HashMap::new();
                    update.insert(result_variable, value);
                    script_update = Some(update);
                }
                Err(err) => {
                    let incident_key = self.mint_key();
                    let reason = format!(
                        "failed to evaluate script expression '{expression}' at script task \
                         '{element_id}': {}",
                        err.0
                    );
                    return (
                        vec![Event::IncidentRaised {
                            incident_key,
                            instance_key,
                            element_instance_key,
                            element_id,
                            kind: state::IncidentKind::ExpressionEvaluation,
                            redrive: None,
                            reason,
                            job_key: None,
                            created_at: self.now,
                        }],
                        Vec::new(),
                    );
                }
            }
        }

        // Business rule task bound to a DMN decision: evaluate it natively now.
        // Like a script task this is a synchronous activity — its result is
        // staged under `result_variable` (or, when absent, a map output's entries
        // are spread) and merged before output mappings run. A failed evaluation
        // (unknown decision, FEEL/hit-policy error) raises a DecisionEvaluation
        // incident and leaves the element active, so resolving the incident
        // re-runs completion (mirrors the script-task/gateway pattern). On
        // success a `DecisionEvaluated` audit event is emitted for the exporter.
        let mut decision_event: Option<Event> = None;
        if let Some(ElementKind::BusinessRuleTask {
            decision_id,
            result_variable,
        }) = self.element_kind(instance_key, &element_id)
        {
            let vars = self.variables(instance_key);
            let resolved_id = self.resolve_job_type(&vars, &decision_id);
            let Some(deployed) = self.state.decisions.get(&resolved_id).cloned() else {
                let incident_key = self.mint_key();
                return (
                    vec![Event::IncidentRaised {
                        incident_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        kind: state::IncidentKind::DecisionEvaluation,
                        redrive: None,
                        reason: format!(
                            "no deployed decision with id '{resolved_id}' for business rule task \
                             '{}'",
                            decision_id
                        ),
                        job_key: None,
                        created_at: self.now,
                    }],
                    Vec::new(),
                );
            };
            let result = crate::dmn::evaluate(&deployed.drg, &resolved_id, &vars);
            if let Some(failure) = result.failure.clone() {
                // Zeebe parity (#1292): a failed evaluation is still recorded
                // (`DecisionEvaluation:FAILED`, a FAILED decision instance) ahead
                // of the incident; each re-drive after resolution is a new
                // evaluation with its own key.
                let failed = self.decision_evaluated_event(
                    &deployed,
                    result,
                    instance_key,
                    element_instance_key,
                    element_id.clone(),
                );
                let incident_key = self.mint_key();
                return (
                    vec![
                        failed,
                        Event::IncidentRaised {
                            incident_key,
                            instance_key,
                            element_instance_key,
                            element_id: element_id.clone(),
                            kind: state::IncidentKind::DecisionEvaluation,
                            redrive: None,
                            reason: format!(
                                "failed to evaluate decision '{}' at business rule task \
                                 '{element_id}': {}",
                                failure.failed_decision_id, failure.message
                            ),
                            job_key: None,
                            created_at: self.now,
                        },
                    ],
                    Vec::new(),
                );
            }
            // Merge the output into the instance: an explicit result variable wraps
            // the output under that name; without one, a map output is spread into
            // the scope (Zeebe requires a resultVariable for a scalar output, but
            // spreading a context output is the natural no-name behaviour).
            let mut update = HashMap::new();
            match &result_variable {
                Some(name) if !name.is_empty() => {
                    update.insert(name.clone(), result.decision_output.clone());
                }
                _ => {
                    if let Value::Map(entries) = &result.decision_output {
                        for (k, v) in entries.iter() {
                            update.insert(k.clone(), v.clone());
                        }
                    }
                }
            }
            if !update.is_empty() {
                script_update = Some(update);
            }
            decision_event = Some(self.decision_evaluated_event(
                &deployed,
                result,
                instance_key,
                element_instance_key,
                element_id.clone(),
            ));
        }

        // Output mappings (zeebe:output): evaluated BEFORE the element completes so
        // a source expression that fails to evaluate (a FEEL parse/type error, or
        // an operation on a missing value like `"x" + missingVar`) halts the
        // element with an incident rather than silently dropping the target and
        // proceeding with it unset (#939). Evaluate against the variables visible
        // to this element instance — its own scope (including any input-mapped
        // locals) layered over the enclosing scopes, plus any job/message result
        // merged on completion, or a script task's result staged above. None of
        // the completion events built below are applied to state until this step
        // returns, so evaluating here sees the same variable view as merging below.
        let outputs = self.io_outputs(instance_key, &element_id);
        let output_updates = if outputs.is_empty() {
            HashMap::new()
        } else {
            let visible = self.variables_for_element(instance_key, element_instance_key);
            let eval = match &script_update {
                // The script result event is not applied to state until this
                // step returns, so overlay it onto the eval context by hand.
                Some(update) => {
                    let mut vars = (*visible).clone();
                    vars.extend(update.clone());
                    self.eval_io_mappings_in(&vars, &outputs)
                }
                None => self.eval_io_mappings_in(&visible, &outputs),
            };
            match eval {
                Ok(updates) => updates,
                Err(failure) => {
                    // Halt in the COMPLETING phase: the element stays active and
                    // parks on the incident, re-driven by `Complete` on resolution
                    // (the `Completion` re-drive) — the same lifecycle as a
                    // script/decision output failure in this function. The single
                    // `IoMapping` kind (REST `IO_MAPPING_ERROR`) carries the
                    // `Completion` phase so resolution re-projects the output
                    // without re-running the element's behaviour, whereas an
                    // input-mapping failure carries `Activation` and re-drives the
                    // activation body. Both surface the one `IO_MAPPING_ERROR`
                    // taxonomy (Zeebe parity: input and output mapping failures
                    // share it, re-driven uniformly by lifecycle phase).
                    // A businessRuleTask's decision WAS evaluated: record it ahead
                    // of the incident rather than dropping it (#1292 — every
                    // evaluation is a decision instance).
                    let mut events: Vec<Event> = decision_event.into_iter().collect();
                    events.push(self.io_mapping_incident(
                        instance_key,
                        element_instance_key,
                        element_id,
                        failure,
                        state::IoMappingRedrive::Completion,
                    ));
                    return (events, Vec::new());
                }
            }
        };

        // Default behaviour: complete and take every outgoing flow (a single flow
        // for ordinary elements; all flows for a parallel split).
        //
        // End execution listeners (ADR 0037): when the element declares `end`
        // listeners, defer `ElementCompleted` and the outgoing flows. The element
        // rests in COMPLETING while a sequential chain of end-listener jobs runs;
        // the deferred completion is emitted by `finalize_completion` once the
        // chain drains (see `advance_listener`). The parked listener job keeps the
        // element active, so `complete_finished_instances` won't finish the
        // instance mid-chain. A listener-free element pushes `ElementCompleted`
        // immediately, keeping its journal byte-identical.
        let end_listeners = self.listeners_of(
            instance_key,
            &element_id,
            crate::model::ListenerEventType::End,
        );
        let mut events = vec![Event::ElementCompleting {
            instance_key,
            element_instance_key,
            element_id: element_id.clone(),
        }];
        if end_listeners.is_empty() {
            events.push(Event::ElementCompleted {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
            });
        }
        // Emit the decision-evaluation audit record (after ElementCompleted, before
        // the variables it produced are merged).
        if let Some(event) = decision_event {
            events.push(event);
        }
        // If this element was an activity guarded by interrupting boundary timers,
        // completing it normally disarms them (and any boundary message subs).
        events.extend(self.cancel_boundary_timers_on(element_instance_key));
        events.extend(self.cancel_boundary_message_subscriptions_on(element_instance_key));
        events.extend(self.cancel_boundary_signal_subscriptions_on(element_instance_key));
        events.extend(self.cancel_boundary_conditional_subscriptions_on(element_instance_key));
        // Event-based gateway deferred choice: when this completing element is
        // the catch event that won the race downstream of an event-based
        // gateway, withdraw the losing sibling catch events (cancel their armed
        // timers/subscriptions and consume their tokens) so only the winning
        // branch continues. A no-op for any element not fed by such a gateway.
        events.extend(self.withdraw_event_gateway_siblings(
            instance_key,
            element_instance_key,
            &element_id,
            scope,
        ));
        // A script task's (or business rule task's) result is merged before output
        // mappings so a `zeebe:output` can reference/remap it (Zeebe merges
        // `resultVariable` first, then applies output mappings).
        if let Some(update) = &script_update {
            events.push(Event::VariablesUpdated {
                instance_key,
                variables: update.clone(),
            });
        }
        // Output mappings (zeebe:output): merge the projection evaluated above
        // (before the completion events) into the element's enclosing (flow) scope
        // and upward — each name updates the nearest ancestor scope that defines
        // it, defaulting to root. The element's own scope is being torn down by
        // `ElementCompleted` (built earlier in this vec), so propagating from the
        // parent avoids writing into the dying scope. Root-only instances collapse
        // to a single flat `VariablesUpdated`, unchanged from the flat engine.
        if !output_updates.is_empty() {
            let flow_scope = self.scope_of(instance_key, element_instance_key);
            events.extend(self.propagated_updates(instance_key, flow_scope, output_updates, false));
        }
        let scope = self.scope_of(instance_key, element_instance_key);

        // End-listener gate: the element rests in COMPLETING. Mint the first
        // end-listener job (its type/retries resolved against the element's
        // completion-time variable view, including any script/DMN result and
        // output mappings staged above), and defer `ElementCompleted` + the
        // outgoing flows to `finalize_completion` when the chain drains.
        if let Some(first) = end_listeners.first() {
            let mut listener_vars =
                (*self.variables_for_element(instance_key, element_instance_key)).clone();
            if let Some(update) = &script_update {
                listener_vars.extend(update.clone());
            }
            let job_key = self.mint_key();
            let job_type = self.resolve_job_type(&listener_vars, &first.job_type);
            let retries = self.resolve_retries(&listener_vars, first.retries.as_deref());
            events.push(Event::ExecutionListenerJobCreated {
                job_key,
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
                job_type,
                event_type: crate::model::ListenerEventType::End,
                listener_index: 0,
                scope,
                created_at: self.now,
                retries,
            });
            return (events, Vec::new());
        }

        // Compensation routing (inline, listener-free path). A completing
        // compensation handler routes back to the throw event waiting on it (its
        // `ElementCompleted` was emitted inline above); a completing compensable
        // activity records a compensation subscription before its outgoing flow.
        if let Some(throw_eik) = self.pending_compensation_handler(instance_key, &element_id, scope)
        {
            let (resume_events, resume_followups) =
                self.resume_compensation_throw(instance_key, &element_id, throw_eik);
            events.extend(resume_events);
            return (events, resume_followups);
        }
        for handler in self.compensation_handlers_for(instance_key, &element_id) {
            events.push(Event::CompensationSubscriptionCreated {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
                handler,
                scope,
            });
        }

        // A link intermediate *throw* event has no outgoing sequence flow: it
        // hands its token to the matching link *catch* event (same link name,
        // same scope), activating it directly. Without this the throw completes
        // into the empty outgoing loop below and the token silently vanishes
        // (#1157). Mirrors the deferred `finalize_completion` handoff for the
        // end-listener path.
        if let Some(ElementKind::LinkIntermediateThrowEvent { link_name }) =
            self.element_kind(instance_key, &element_id)
        {
            let mut followups = Vec::new();
            if let Some(catch_id) = self.resolve_link_catch(instance_key, &element_id, &link_name) {
                followups.push(Step::Activate {
                    instance_key,
                    element_id: catch_id,
                    scope,
                    via: None,
                });
            }
            return (events, followups);
        }

        // An escalation throw event (`escalationEventDefinition` on an
        // intermediate throw or end event, #1173): raise the named escalation
        // and continue. Mirrors the deferred `finalize_completion` handoff for
        // the end-listener path — escalation is non-critical, so the throw's own
        // token always routes onward / drains regardless of whether it is caught.
        if let Some(ElementKind::EscalationThrowEvent { escalation_code }) =
            self.element_kind(instance_key, &element_id)
        {
            return self.raise_escalation_from_throw(
                instance_key,
                element_instance_key,
                element_id,
                scope,
                escalation_code,
                events,
            );
        }

        let mut followups = Vec::new();
        for (taken, step) in self.take_all_flows(instance_key, &element_id, scope) {
            events.extend(taken);
            followups.push(step);
        }
        (events, followups)
    }

    /// Mints the first `end` execution-listener job for an element that has just
    /// entered COMPLETING (ADR 0037), if it declares any. Returns the
    /// `ExecutionListenerJobCreated` event when there is an end chain to run — the
    /// caller appends it, having already emitted `ElementCompleting` and its
    /// completing-time work (output mappings, result merge, boundary disarm), and
    /// then defers `ElementCompleted` + its structural downstream to the matching
    /// `finalize_*` (dispatched by [`finalize_end_transition`] once the chain
    /// drains). Returns `None` when the element has no end listeners, so the
    /// caller completes inline and its journal stays byte-identical.
    ///
    /// `vars` is the element's completion-time variable view (its own scope,
    /// including any input-mapped locals and merged job/script/DMN result), which
    /// the listener job's own FEEL attributes (`type`, `retries`) resolve against.
    fn begin_end_listener_chain(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
        scope: Key,
        vars: &HashMap<String, Value>,
    ) -> Option<Event> {
        let end_listeners = self.listeners_of(
            instance_key,
            element_id,
            crate::model::ListenerEventType::End,
        );
        let first = end_listeners.first()?;
        let job_key = self.mint_key();
        let job_type = self.resolve_job_type(vars, &first.job_type);
        let retries = self.resolve_retries(vars, first.retries.as_deref());
        Some(Event::ExecutionListenerJobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id: element_id.to_string(),
            job_type,
            event_type: crate::model::ListenerEventType::End,
            listener_index: 0,
            scope,
            created_at: self.now,
            retries,
        })
    }

    /// Dispatches the deferred completion of an element whose `end`
    /// execution-listener chain has drained (ADR 0037) to the finalizer matching
    /// the completion site that parked it. The parked element still rests in
    /// COMPLETING with its scope resident, so each finalizer re-derives its
    /// structural tail (`ElementCompleted` + the site's own downstream) from the
    /// current state — no captured context, replay-safe, exactly like the `start`
    /// side re-derives [`run_activation_body`]. Ordinary elements (service/user/
    /// script/business-rule tasks, pass-through events) fall through to
    /// [`finalize_completion`].
    fn finalize_end_transition(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        // A multi-instance body (its own instance key is the body scope key).
        if self
            .state
            .instances
            .get(&instance_key)
            .map(|i| i.multi_instances.contains_key(&element_instance_key))
            .unwrap_or(false)
        {
            return self.finalize_multi_instance_body(instance_key, element_instance_key);
        }
        // An ad-hoc sub-process container (keyed by its own instance key).
        if self
            .state
            .instances
            .get(&instance_key)
            .map(|i| i.adhoc_instances.contains_key(&element_instance_key))
            .unwrap_or(false)
        {
            return self.finalize_adhoc_container(instance_key, element_instance_key, false);
        }
        match self.element_kind(instance_key, &element_id) {
            Some(ElementKind::ExclusiveGateway) => self.finalize_exclusive_gateway(
                instance_key,
                element_instance_key,
                element_id,
                scope,
            ),
            Some(ElementKind::InclusiveGateway) => self.finalize_inclusive_gateway(
                instance_key,
                element_instance_key,
                element_id,
                scope,
            ),
            Some(ElementKind::SubProcess { .. }) => {
                self.finalize_subprocess(instance_key, element_instance_key, element_id, scope)
            }
            _ => self.finalize_completion(instance_key, element_instance_key, element_id, scope),
        }
    }

    /// Emits the deferred completion of an element whose `end` execution-listener
    /// chain has drained (ADR 0037): `ElementCompleted` followed by taking every
    /// outgoing flow. The completing-time work (boundary disarm, result merge,
    /// output mappings) already ran in [`complete`]; this is only the tail that
    /// was parked behind the listener chain.
    fn finalize_completion(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        // A compensation handler completing routes back to the compensation
        // throw event waiting on it, not along (non-existent) outgoing flows.
        if let Some(throw_eik) = self.pending_compensation_handler(instance_key, &element_id, scope)
        {
            return self.finalize_compensation_handler(
                instance_key,
                element_instance_key,
                element_id,
                throw_eik,
            );
        }
        let mut events = vec![Event::ElementCompleted {
            instance_key,
            element_instance_key,
            element_id: element_id.clone(),
        }];
        let mut followups = Vec::new();
        // A link intermediate *throw* event has no outgoing sequence flow: on
        // completion it hands its token to the matching link *catch* event (same
        // link name, same scope), activating it directly. Without this the throw
        // completes into the pass-through outgoing loop below, finds no flow, and
        // the token silently vanishes (#1157).
        if let Some(ElementKind::LinkIntermediateThrowEvent { link_name }) =
            self.element_kind(instance_key, &element_id)
        {
            if let Some(catch_id) = self.resolve_link_catch(instance_key, &element_id, &link_name) {
                followups.push(Step::Activate {
                    instance_key,
                    element_id: catch_id,
                    scope,
                    via: None,
                });
            }
            return (events, followups);
        }
        // An escalation throw event (`escalationEventDefinition` on an
        // intermediate throw or end event, #1173): raise the named escalation,
        // then continue. Escalation is non-critical, so the throw's own token
        // always routes onward / drains regardless of whether it was caught.
        if let Some(ElementKind::EscalationThrowEvent { escalation_code }) =
            self.element_kind(instance_key, &element_id)
        {
            return self.raise_escalation_from_throw(
                instance_key,
                element_instance_key,
                element_id,
                scope,
                escalation_code,
                events,
            );
        }
        for handler in self.compensation_handlers_for(instance_key, &element_id) {
            events.push(Event::CompensationSubscriptionCreated {
                instance_key,
                element_instance_key,
                element_id: element_id.clone(),
                handler,
                scope,
            });
        }
        for (taken, step) in self.take_all_flows(instance_key, &element_id, scope) {
            events.extend(taken);
            followups.push(step);
        }
        (events, followups)
    }

    /// Raises the escalation of a just-completed
    /// [`EscalationThrowEvent`](ElementKind::EscalationThrowEvent). `events`
    /// already carries the throw's own `ElementCompleted`. Escalation is
    /// non-critical, so:
    ///
    /// * **uncaught** — the throw simply routes onward / drains (as a plain
    ///   pass-through), no incident;
    /// * **caught by a non-interrupting boundary** — the throw still routes
    ///   onward, and a parallel token is spawned by activating the boundary in
    ///   its own scope (the enclosing activity keeps running);
    /// * **caught by an interrupting boundary** — the caught activity's whole
    ///   inner scope is torn down (mirroring the error-boundary path) and the
    ///   token continues out the boundary instead of along the throw's flow.
    ///
    /// The catch is resolved by propagating up the enclosing sub-process scopes
    /// from the throw's `scope`, preferring an exact escalation-code match over a
    /// catch-all (see [`find_catching_escalation_boundary`](Self::find_catching_escalation_boundary)).
    fn raise_escalation_from_throw(
        &mut self,
        instance_key: Key,
        throw_eik: Key,
        throw_element_id: String,
        scope: Key,
        escalation_code: String,
        mut events: Vec<Event>,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut followups = Vec::new();
        let caught = self.find_catching_escalation_boundary(instance_key, scope, &escalation_code);
        // Build the throw's outgoing flows up front (owned) so routing does not
        // hold an immutable borrow of `self` across the teardown mutation.
        let outgoing_flows = self.take_all_flows(instance_key, &throw_element_id, scope);
        let take_throw_outgoing = |events: &mut Vec<Event>, followups: &mut Vec<Step>| {
            for (taken, step) in outgoing_flows {
                events.extend(taken);
                followups.push(step);
            }
        };
        match caught {
            // Uncaught: ignore the escalation (no incident) and pass through.
            None => {
                take_throw_outgoing(&mut events, &mut followups);
            }
            Some((boundary_id, caught_eik, caught_element_id, false)) => {
                // Non-interrupting: the throw continues AND a parallel token is
                // spawned by activating the boundary in the caught activity's
                // own (enclosing) scope.
                //
                // If the caught activity is a multi-instance CHILD, the boundary
                // is armed on the loop's BODY, not the individual child — so the
                // parallel token must be spawned in the BODY's enclosing scope.
                // `scope_of(caught_eik)` for an MI child is the MI body itself,
                // which would activate the handler INSIDE the loop body (the
                // wrong level). Mirror the interrupting branch's child→body
                // redirection before taking `scope_of` so both branches arm the
                // boundary at the same scope (#1173, the #1170 MI class).
                take_throw_outgoing(&mut events, &mut followups);
                let child_scope = self.scope_of(instance_key, caught_eik);
                let caught_is_mi_child = self
                    .state
                    .instances
                    .get(&instance_key)
                    .is_some_and(|i| i.multi_instances.contains_key(&child_scope));
                let boundary_armed_eik = if caught_is_mi_child {
                    child_scope
                } else {
                    caught_eik
                };
                let boundary_scope = self.scope_of(instance_key, boundary_armed_eik);
                followups.push(Step::Activate {
                    instance_key,
                    element_id: boundary_id,
                    scope: boundary_scope,
                    via: None,
                });
                let _ = caught_element_id;
            }
            Some((boundary_id, caught_eik, caught_element_id, true)) => {
                // Interrupting: tear the caught sub-process scope down (its
                // descendants, but not the throw itself — its `ElementCompleted`
                // is already in `events`), complete the caught activity, and run
                // the token out the boundary instead of along the throw's flow.
                //
                // If the caught activity is a multi-instance CHILD, the boundary
                // is armed on the loop's BODY, so interrupting it must tear down
                // the WHOLE loop — every sibling child and the body's runtime
                // record — not the single child the throw happened to fire from.
                // Redirect the teardown to the MI body (its element id equals the
                // child's, since each child is an instance of the same activity);
                // otherwise sibling children and the body's `MultiInstanceState`
                // survive and hang the instance (#1173, the #1170 MI class).
                let child_scope = self.scope_of(instance_key, caught_eik);
                let caught_is_mi_child = self
                    .state
                    .instances
                    .get(&instance_key)
                    .is_some_and(|i| i.multi_instances.contains_key(&child_scope));
                let (torn_eik, torn_element_id) = if caught_is_mi_child {
                    let body_element_id = self
                        .element_id_of_instance(instance_key, child_scope)
                        .unwrap_or_else(|| caught_element_id.clone());
                    (child_scope, body_element_id)
                } else {
                    (caught_eik, caught_element_id)
                };
                let boundary_scope = self.scope_of(instance_key, torn_eik);
                for event in self.scope_teardown_events(instance_key, torn_eik) {
                    if matches!(
                        &event,
                        Event::ElementCompleting { element_instance_key: eik, .. }
                            | Event::ElementCompleted { element_instance_key: eik, .. }
                            if *eik == throw_eik
                    ) {
                        continue;
                    }
                    events.push(event);
                }
                // `scope_teardown_events` only sweeps the torn activity's
                // DESCENDANTS. If the torn activity is itself a multi-instance
                // body or an ad-hoc container, its own active-child runtime
                // record (cleared only by `MultiInstanceCompleted` /
                // `AdHocCompleted`, never by `ElementCompleted`) — plus any job
                // or incident parked on the root — would otherwise leak, hanging
                // the instance or letting a queued child activation resurrect
                // work after the handler. Mirror `interrupt_activity_via_boundary`'s
                // root-record cleanup (#1173, matching the #1170 MI-teardown fix).
                for event in self.boundary_root_teardown_events(instance_key, torn_eik) {
                    events.push(event);
                }
                self.torn_down_scopes.insert(torn_eik);
                events.push(Event::ElementCompleting {
                    instance_key,
                    element_instance_key: torn_eik,
                    element_id: torn_element_id.clone(),
                });
                events.push(Event::ElementCompleted {
                    instance_key,
                    element_instance_key: torn_eik,
                    element_id: torn_element_id,
                });
                for event in self.cancel_boundary_timers_on(torn_eik) {
                    events.push(event);
                }
                for event in self.cancel_boundary_message_subscriptions_on(torn_eik) {
                    events.push(event);
                }
                for event in self.cancel_boundary_signal_subscriptions_on(torn_eik) {
                    events.push(event);
                }
                for event in self.cancel_boundary_conditional_subscriptions_on(torn_eik) {
                    events.push(event);
                }
                followups.push(Step::Activate {
                    instance_key,
                    element_id: boundary_id,
                    scope: boundary_scope,
                    via: None,
                });
            }
        }
        (events, followups)
    }

    /// Completes a compensation handler and, once its compensation throw event
    /// has no outstanding handlers left, completes that throw event and routes
    /// its token onward (an `intermediateThrowEvent`) or drains it (an
    /// `endEvent`). The handler carries no outgoing flow of its own.
    fn finalize_compensation_handler(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        throw_eik: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut events = vec![Event::ElementCompleted {
            instance_key,
            element_instance_key,
            element_id: element_id.clone(),
        }];
        let (resume_events, followups) =
            self.resume_compensation_throw(instance_key, &element_id, throw_eik);
        events.extend(resume_events);
        (events, followups)
    }

    /// Records that a compensation handler (already emitted its own
    /// `ElementCompleted` by the caller) finished, and — when it was the throw
    /// event's last outstanding handler — completes the compensation throw event
    /// and routes its token onward. Shared by the inline ([`complete`]) and
    /// deferred ([`finalize_compensation_handler`]) completion paths.
    fn resume_compensation_throw(
        &self,
        instance_key: Key,
        handler_element_id: &str,
        throw_eik: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let wait = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.compensation_waits.get(&throw_eik));
        let (throw_element_id, last_handler) = match wait {
            Some(wait) => (
                wait.throw_element_id.clone(),
                wait.pending_handlers.len() == 1,
            ),
            None => return (Vec::new(), Vec::new()),
        };
        let events = vec![Event::CompensationHandlerCompleted {
            instance_key,
            throw_element_instance_key: throw_eik,
            handler_element_id: handler_element_id.to_string(),
        }];
        let mut followups = Vec::new();
        if last_handler {
            // The throw's outstanding handlers are all done: complete it through
            // the normal completion pipeline (`Step::Complete`), exactly like the
            // nothing-to-compensate pass-through path (see `run_activation_body`).
            // Completing inline here would bypass the throw's own end execution
            // listeners, output mappings and boundary disarm; routing through
            // `complete` keeps throw completion semantics consistent with every
            // other element.
            followups.push(Step::Complete {
                instance_key,
                element_instance_key: throw_eik,
                element_id: throw_element_id,
            });
        }
        (events, followups)
    }

    /// Withdraws the losing siblings of an event-based gateway's deferred choice.
    ///
    /// An event-based gateway routes into several intermediate catch events and
    /// arms them all at once (its completion takes every outgoing flow). The
    /// first event to occur wins: this helper is called when that winning catch
    /// event completes and tears down the *other* targets of the same gateway —
    /// cancelling every armed timer and open (message/signal/conditional)
    /// subscription resting on each losing sibling, then completing its element
    /// instance so its token is consumed without taking an outgoing flow. Only
    /// active sibling instances in the same token `scope` are withdrawn, so a
    /// gateway reached again on a loop only ever withdraws the current race.
    ///
    /// Returns an empty vec — the overwhelmingly common path — when `winner`
    /// is not the immediate target of an event-based gateway, or when its owning
    /// gateway is ambiguous (more than one event-based gateway routes into the
    /// same catch event), in which case no siblings are withdrawn.
    fn withdraw_event_gateway_siblings(
        &self,
        instance_key: Key,
        winner_eik: Key,
        winner_element_id: &str,
        scope: Key,
    ) -> Vec<Event> {
        let Some(def) = self.process_of_instance(instance_key) else {
            return Vec::new();
        };
        // An event-based gateway's deferred choice is a race between intermediate
        // catch events only. `is_catch_event` gates both the winner (below) and
        // each losing sibling: it keeps the completion hot path O(1) for the
        // common (non-catch) completion, and — should a malformed model route a
        // gateway into a non-catch node (e.g. a service task) — prevents this
        // code from force-completing that node's element instance via
        // `ElementCompleted` while leaving its job/user-task uncancelled (which
        // would orphan work). Withdrawal only ever cancels timers/subscriptions
        // resting on genuine catch siblings.
        let is_catch_event = |element_id: &str| {
            matches!(
                def.elements.get(element_id).map(|e| &e.kind),
                Some(
                    ElementKind::TimerIntermediateCatchEvent { .. }
                        | ElementKind::MessageIntermediateCatchEvent { .. }
                        | ElementKind::SignalIntermediateCatchEvent { .. }
                        | ElementKind::ConditionalIntermediateCatchEvent { .. }
                )
            )
        };
        if !is_catch_event(winner_element_id) {
            return Vec::new();
        }
        // A catch event downstream of an event-based gateway has exactly one
        // incoming flow — from the gateway. If the winner has any *other*
        // incoming path (a malformed graph where a non-gateway node also routes
        // into it), we cannot be sure this token arrived via the gateway, so we
        // conservatively withdraw nothing rather than risk cancelling an
        // unrelated race in the same scope.
        let incoming_count = def
            .elements
            .values()
            .flat_map(|element| element.outgoing.iter())
            .filter(|f| f.to == winner_element_id)
            .count();
        if incoming_count != 1 {
            return Vec::new();
        }
        // Find the event-based gateway(s) that route into the winning catch
        // event. In a well-formed model a catch event has exactly one incoming
        // flow, so at most one gateway owns the race. If more than one gateway
        // routes into the same catch event the owning race is ambiguous — we
        // cannot tell which gateway's siblings to withdraw without risking the
        // cancellation of an unrelated gateway's branch — so we conservatively
        // do nothing.
        let owners: Vec<&crate::model::Element> = def
            .elements
            .values()
            .filter(|element| matches!(element.kind, ElementKind::EventBasedGateway))
            .filter(|element| element.outgoing.iter().any(|f| f.to == winner_element_id))
            .collect();
        let [owner] = owners.as_slice() else {
            return Vec::new();
        };
        // The owning gateway must itself have exactly one incoming flow. A
        // gateway reachable via multiple incoming flows (e.g. fed by both arms of
        // a parallel split) can be activated concurrently in the same scope,
        // arming several live instances of each sibling element id at once. Since
        // losers are matched by element id + scope, withdrawing here could cancel
        // a sibling instance belonging to a *different* concurrent activation of
        // the same gateway. When the owner's static in-degree is not exactly 1 we
        // cannot isolate a single race, so we conservatively withdraw nothing.
        let owner_incoming = def
            .elements
            .values()
            .flat_map(|element| element.outgoing.iter())
            .filter(|f| f.to == owner.id)
            .count();
        if owner_incoming != 1 {
            return Vec::new();
        }
        // The sibling targets are the owning gateway's *other* outgoing catch
        // events. Any non-catch target is skipped (see `is_catch_event` above).
        let sibling_ids: Vec<&str> = owner
            .outgoing
            .iter()
            .filter(|f| f.to != winner_element_id)
            .map(|f| f.to.as_str())
            .filter(|id| is_catch_event(id))
            .collect();
        if sibling_ids.is_empty() {
            return Vec::new();
        }
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Vec::new();
        };
        // Every active element instance of a losing sibling in this token scope.
        // Sorted by element-instance key for a deterministic, replay-stable log.
        let mut losers: Vec<(Key, String)> = instance
            .active
            .iter()
            .filter(|(eik, eid)| {
                **eik != winner_eik
                    && sibling_ids.contains(&eid.as_str())
                    && self.scope_of(instance_key, **eik) == scope
            })
            .map(|(eik, eid)| (*eik, eid.clone()))
            .collect();
        losers.sort_by_key(|(eik, _)| *eik);

        let mut events = Vec::new();
        for (eik, eid) in losers {
            events.extend(self.cancel_all_timers_on(eik));
            events.extend(self.cancel_all_subscriptions_on(eik));
            events.extend(self.cancel_all_signal_subscriptions_on(eik));
            events.extend(self.cancel_all_conditional_subscriptions_on(eik));
            events.push(Event::ElementCompleting {
                instance_key,
                element_instance_key: eik,
                element_id: eid.clone(),
            });
            events.push(Event::ElementCompleted {
                instance_key,
                element_instance_key: eik,
                element_id: eid,
            });
        }
        events
    }
    /// whose `end` execution-listener chain has drained (ADR 0037). Its boundary
    /// events disarmed and its output mappings projected when it parked (in the
    /// drained-sub-process sweep); this emits the parked `ElementCompleted` and
    /// takes its outgoing flows. A newly-drained *enclosing* sub-process is picked
    /// up by the end-of-command sweep, so nesting composes.
    fn finalize_subprocess(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut events = vec![Event::ElementCompleted {
            instance_key,
            element_instance_key,
            element_id: element_id.clone(),
        }];
        let mut followups = Vec::new();
        for (taken, step) in self.take_all_flows(instance_key, &element_id, scope) {
            events.extend(taken);
            followups.push(step);
        }
        (events, followups)
    }

    /// Completes a terminate end event (`ElementKind::TerminateEndEvent`): it
    /// kills every other still-active token in its enclosing scope and then
    /// completes that scope (Zeebe/Camunda terminate semantics). `scope` is the
    /// terminate-end's enclosing scope — `0` for the top-level process, otherwise
    /// the enclosing sub-process element-instance key.
    ///
    /// * Top-level (`scope == 0`): the terminate-end completes, every other token
    ///   in the instance is discarded (its jobs/timers/subscriptions/user tasks
    ///   cancelled, its call-activity children terminated) and the whole instance
    ///   ends via `ProcessInstanceCompleted` (Zeebe parity, #1085: only the inner
    ///   element instances record TERMINATED; the process instance's own terminal
    ///   record is `ELEMENT_COMPLETED`). If this instance is itself a called
    ///   process (spawned by a call activity), its completion ends **only** this
    ///   instance — the parent's call activity completes normally and the parent
    ///   continues on its outgoing flow (Zeebe/BPMN: a terminate end never
    ///   propagates out of the process it fires in).
    /// * Sub-process-scoped (`scope != 0`): only the tokens inside that
    ///   sub-process scope die (this terminate-end among them); the now-childless
    ///   sub-process is then completed by the ordinary post-drain
    ///   [`complete_drained_subprocesses`](Self::complete_drained_subprocesses)
    ///   sweep — through its real completion path (plain / multi-instance child /
    ///   ad-hoc tool, with its output mappings, end-listener chain and boundary
    ///   disarm) — which also continues the parent on the sub-process's outgoing
    ///   flow. Deferring to that single completion path (rather than hand-building
    ///   it here) keeps one source of truth and avoids mis-handling an MI/ad-hoc
    ///   sub-process body.
    ///
    /// Returns events without emitting them so it composes inside the decide-only
    /// `process_step` result, reusing the same teardown primitives as the
    /// error-boundary / cancel paths ([`scope_teardown_events`],
    /// [`discard_instance_events`]).
    fn complete_terminate_end(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        if scope == 0 {
            // Top-level terminate end: complete the terminate-end itself, then
            // discard every remaining token and COMPLETE the whole instance.
            // Zeebe parity (#1085): the terminate end kills every inner element
            // instance (its jobs/timers/subscriptions/user tasks cancelled, its
            // call-activity children terminated by the post-drain cascade) — only
            // those inner scopes record TERMINATED — but the process instance's
            // own terminal record is `ProcessInstanceCompleted`, matching Zeebe's
            // `PROCESS -> ELEMENT_COMPLETED`.
            // Capture the parent-release step (if this is a called process) now,
            // while the child's variables and the parent's call-activity token
            // are still live — the events below are decided, not yet applied.
            let release_parent = self.call_activity_completion_step(instance_key);
            let mut events = vec![
                Event::ElementCompleting {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                },
                Event::ElementCompleted {
                    instance_key,
                    element_instance_key,
                    element_id,
                },
            ];
            events.extend(self.discard_instance_token_events(instance_key));
            // Resolve every incident still open on the instance before the
            // terminal record. The `ProcessInstanceTerminated` reducer used to
            // close these implicitly; the parity switch to `ProcessInstanceCompleted`
            // (whose reducer must NOT close incidents — a normal completion may
            // retain one for later resolution) means the terminate end must resolve
            // them itself. Emitted after the `JobCanceled`s above so a failed
            // job's `IncidentResolved` cannot resurrect the just-cancelled job
            // (the reducer only re-pools a still-`Failed` job).
            events.extend(self.resolve_instance_incidents(instance_key));
            events.push(Event::ProcessInstanceCompleted { instance_key });
            return (events, release_parent.into_iter().collect());
        }

        // Sub-process-scoped terminate end: tear down every token inside the
        // enclosing sub-process scope (this terminate-end included, as a
        // descendant of `scope`). The now-childless sub-process is completed by
        // the post-drain `complete_drained_subprocesses` sweep via its ordinary
        // completion path, which also continues the parent on the sub-process's
        // outgoing flow — one source of truth for sub-process completion.
        //
        // Record the scope as torn down for this drain so a sibling branch's
        // `Step::Activate` still queued behind this terminate cannot recreate a
        // token inside the dead scope (which would keep the sub-process from ever
        // draining) — the sub-process token stays active until the sweep, so the
        // dead-scope guard needs this marker, not just the `active` map.
        self.torn_down_scopes.insert(scope);
        (self.scope_teardown_events(instance_key, scope), Vec::new())
    }
    /// completed (ADR 0037). If another listener remains in the chain, its job is
    /// created; otherwise the chain has drained and the deferred lifecycle
    /// transition runs — a `Start` chain enacts the element's activation
    /// behaviour ([`run_activation_body`]), an `End` chain emits the deferred
    /// completion ([`finalize_completion`]).
    fn advance_listener(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        event_type: crate::model::ListenerEventType,
        index: usize,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let listeners = self.listeners_of(instance_key, &element_id, event_type);
        let next_index = index + 1;
        if let Some(next) = listeners.get(next_index) {
            // The element's applied scope view (input mappings for a start chain,
            // completion-time state for an end chain, plus any variables merged by
            // preceding listener completions) is what the next listener's FEEL
            // attributes resolve against.
            let listener_vars = self.variables_for_element(instance_key, element_instance_key);
            let job_key = self.mint_key();
            let job_type = self.resolve_job_type(&listener_vars, &next.job_type);
            let retries = self.resolve_retries(&listener_vars, next.retries.as_deref());
            return (
                vec![Event::ExecutionListenerJobCreated {
                    job_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    job_type,
                    event_type,
                    listener_index: next_index,
                    scope,
                    created_at: self.now,
                    retries,
                }],
                Vec::new(),
            );
        }

        // Chain drained — run the deferred transition.
        match event_type {
            crate::model::ListenerEventType::Start => {
                // A multi-instance body deferred its child fan-out (not
                // `run_activation_body`): its activation already ran in
                // `activate_multi_instance_body`; the start chain gated only the
                // spawn, re-derived here from the body record.
                if self
                    .state
                    .instances
                    .get(&instance_key)
                    .map(|i| i.multi_instances.contains_key(&element_instance_key))
                    .unwrap_or(false)
                {
                    (
                        Vec::new(),
                        self.spawn_multi_instance_children(instance_key, element_instance_key),
                    )
                } else {
                    let vars = self.variables_for_element(instance_key, element_instance_key);
                    self.run_activation_body(
                        instance_key,
                        element_id,
                        element_instance_key,
                        scope,
                        vars,
                    )
                }
            }
            crate::model::ListenerEventType::End => {
                self.finalize_end_transition(instance_key, element_instance_key, element_id, scope)
            }
        }
    }

    /// Whether a task-listener event type may *deny* its transition. Only
    /// `assigning`, `updating` and `completing` support denial; `creating` and
    /// `canceling` must proceed (Zeebe parity, ADR 0037 §6).
    fn task_event_supports_deny(event_type: crate::model::TaskListenerEventType) -> bool {
        matches!(
            event_type,
            crate::model::TaskListenerEventType::Assigning
                | crate::model::TaskListenerEventType::Updating
                | crate::model::TaskListenerEventType::Completing
        )
    }

    /// Builds the events that begin a user task's task-listener chain: records
    /// the deferred transition on the task and mints the first listener's job.
    /// The caller has already checked that `first` exists (the listener-free
    /// path never calls this, keeping listener-free user tasks byte-identical).
    #[allow(clippy::too_many_arguments)]
    fn start_task_listener_chain(
        &mut self,
        user_task_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
        event_type: crate::model::TaskListenerEventType,
        pending: state::PendingUserTaskTransition,
        first: &crate::model::TaskListener,
    ) -> Vec<Event> {
        let job_event = self.mint_task_listener_job(
            user_task_key,
            instance_key,
            element_instance_key,
            element_id,
            event_type,
            0,
            first,
        );
        vec![
            Event::UserTaskTransitionDeferred {
                user_task_key,
                instance_key,
                pending,
            },
            job_event,
        ]
    }

    /// Mints a task-listener job for the listener at `index` of `event_type`'s
    /// chain, resolving its `type`/`retries` FEEL against the user task's scope.
    #[allow(clippy::too_many_arguments)]
    fn mint_task_listener_job(
        &mut self,
        user_task_key: Key,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
        event_type: crate::model::TaskListenerEventType,
        index: usize,
        listener: &crate::model::TaskListener,
    ) -> Event {
        let listener_vars = self.variables_for_element(instance_key, element_instance_key);
        let job_key = self.mint_key();
        let job_type = self.resolve_job_type(&listener_vars, &listener.job_type);
        let retries = self.resolve_retries(&listener_vars, listener.retries.as_deref());
        Event::TaskListenerJobCreated {
            job_key,
            instance_key,
            element_instance_key,
            element_id: element_id.to_string(),
            user_task_key,
            job_type,
            event_type,
            listener_index: index,
            created_at: self.now,
            retries,
        }
    }

    /// Advances a user task's task-listener chain after the listener at `index`
    /// completed: mints the next listener's job, or — when the chain drains —
    /// commits the deferred transition (ADR 0037 §6).
    fn advance_task_listener(
        &mut self,
        user_task_key: Key,
        event_type: crate::model::TaskListenerEventType,
        index: usize,
    ) -> (Vec<Event>, Vec<Step>) {
        let Some(task) = self.state.user_tasks.get(&user_task_key) else {
            return (Vec::new(), Vec::new());
        };
        let instance_key = task.instance_key;
        let element_instance_key = task.element_instance_key;
        let element_id = task.element_id.clone();
        let listeners = self.task_listeners_of(instance_key, &element_id, event_type);
        let next_index = index + 1;
        if let Some(next) = listeners.get(next_index).cloned() {
            let job_event = self.mint_task_listener_job(
                user_task_key,
                instance_key,
                element_instance_key,
                &element_id,
                event_type,
                next_index,
                &next,
            );
            return (vec![job_event], Vec::new());
        }
        // Chain drained — commit the deferred transition.
        self.commit_user_task_transition(user_task_key)
    }

    /// Commits a user task's deferred transition once its listener chain has
    /// drained: applies accumulated corrections, emits the appropriate lifecycle
    /// event(s), clears the pending state and resumes the token where the
    /// transition requires it (ADR 0037 §6).
    fn commit_user_task_transition(&mut self, user_task_key: Key) -> (Vec<Event>, Vec<Step>) {
        let Some(task) = self.state.user_tasks.get(&user_task_key) else {
            return (Vec::new(), Vec::new());
        };
        let Some(pending) = task.pending.clone() else {
            return (Vec::new(), Vec::new());
        };
        let instance_key = task.instance_key;
        let element_instance_key = task.element_instance_key;
        let element_id = task.element_id.clone();
        let has_initial_assignee = task.assignee.is_some() || pending.assignee.is_some();
        let corrections = pending.corrections.clone();

        let mut events: Vec<Event> = Vec::new();
        let mut steps: Vec<Step> = Vec::new();
        let mut finish_termination = false;

        match pending.event_type {
            crate::model::TaskListenerEventType::Creating => {
                // The task becomes available. A creating listener's assignee
                // correction was already validated at job completion (rejected
                // if an initial assignee exists), so apply corrections directly.
                events.extend(self.apply_creating_corrections(
                    user_task_key,
                    instance_key,
                    &corrections,
                    has_initial_assignee,
                ));
            }
            crate::model::TaskListenerEventType::Assigning => {
                // The corrected assignee (if any) overrides the command's target.
                let assignee = match &corrections.assignee {
                    Some(a) if a.is_empty() => None,
                    Some(a) => Some(a.clone()),
                    None => pending.assignee.clone(),
                };
                events.extend(self.apply_non_assignee_corrections(
                    user_task_key,
                    instance_key,
                    &corrections,
                ));
                events.push(Event::UserTaskAssigned {
                    user_task_key,
                    instance_key,
                    assignee,
                });
            }
            crate::model::TaskListenerEventType::Updating => {
                let update = pending.update.clone().unwrap_or_default();
                // Corrections override the corresponding update fields.
                events.push(Event::UserTaskUpdated {
                    user_task_key,
                    instance_key,
                    candidate_groups: corrections
                        .candidate_groups
                        .clone()
                        .or(update.candidate_groups),
                    candidate_users: corrections
                        .candidate_users
                        .clone()
                        .or(update.candidate_users),
                    due_date: corrections
                        .due_date
                        .clone()
                        .map(|d| if d.is_empty() { None } else { Some(d) })
                        .or(update.due_date),
                    follow_up_date: corrections
                        .follow_up_date
                        .clone()
                        .map(|d| if d.is_empty() { None } else { Some(d) })
                        .or(update.follow_up_date),
                    priority: corrections.priority.or(update.priority),
                });
                if corrections.assignee.is_some() {
                    let assignee = corrections.assignee.clone().filter(|a| !a.is_empty());
                    events.push(Event::UserTaskAssigned {
                        user_task_key,
                        instance_key,
                        assignee,
                    });
                }
            }
            crate::model::TaskListenerEventType::Completing => {
                // Apply corrections to the (about-to-complete) task's data, then
                // complete it and propagate the captured completion variables.
                events.extend(self.apply_non_assignee_corrections(
                    user_task_key,
                    instance_key,
                    &corrections,
                ));
                if corrections.assignee.is_some() {
                    let assignee = corrections.assignee.clone().filter(|a| !a.is_empty());
                    events.push(Event::UserTaskAssigned {
                        user_task_key,
                        instance_key,
                        assignee,
                    });
                }
                events.push(Event::UserTaskCompleted {
                    user_task_key,
                    instance_key,
                });
                steps.push(Step::Complete {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                });
            }
            crate::model::TaskListenerEventType::Canceling => {
                // Cancellation must proceed; corrections are ignored.
                events.push(Event::UserTaskCanceled {
                    user_task_key,
                    instance_key,
                });
                // If this was the last user task deferring cancellation on a
                // terminating instance, finish the termination now (ADR 0037 §6).
                let terminating = self
                    .state
                    .instances
                    .get(&instance_key)
                    .map(|i| i.state == ProcessInstanceState::Terminating)
                    .unwrap_or(false);
                if terminating {
                    let others_canceling = self.state.user_tasks.values().any(|t| {
                        t.instance_key == instance_key
                            && t.key != user_task_key
                            && matches!(
                                t.pending.as_ref().map(|p| p.event_type),
                                Some(crate::model::TaskListenerEventType::Canceling)
                            )
                    });
                    if !others_canceling {
                        // Emitted after the resolution event below so pending is
                        // cleared first.
                        finish_termination = true;
                    }
                }
            }
        }

        events.push(Event::UserTaskTransitionResolved {
            user_task_key,
            instance_key,
            denied: None,
        });

        if finish_termination {
            events.push(Event::ProcessInstanceTerminated { instance_key });
        }

        // Creating→assigning handoff (Zeebe parity): when the creating chain
        // carried a stripped initial assignee, route it through an assigning
        // transition now that the task is available. Emitted after the resolution
        // event above so the creating pending is cleared before the assigning
        // pending is set on replay.
        if matches!(
            pending.event_type,
            crate::model::TaskListenerEventType::Creating
        ) {
            if let Some(initial) = pending.assignee.clone() {
                let assigning = self.task_listeners_of(
                    instance_key,
                    &element_id,
                    crate::model::TaskListenerEventType::Assigning,
                );
                if let Some(first) = assigning.first().cloned() {
                    let new_pending = state::PendingUserTaskTransition {
                        event_type: crate::model::TaskListenerEventType::Assigning,
                        assignee: Some(initial),
                        update: None,
                        variables: std::collections::HashMap::new(),
                        corrections: crate::model::UserTaskCorrections::default(),
                    };
                    events.extend(self.start_task_listener_chain(
                        user_task_key,
                        instance_key,
                        element_instance_key,
                        &element_id,
                        crate::model::TaskListenerEventType::Assigning,
                        new_pending,
                        &first,
                    ));
                }
            }
        }

        // For completing, the variables must merge before the token resumes; do
        // it now (the variable events precede the resolution/step above only in
        // ordering terms — merge here so replay sees them before the resume).
        if matches!(
            pending.event_type,
            crate::model::TaskListenerEventType::Completing
        ) && !pending.variables.is_empty()
        {
            let flow_scope = self.scope_of(instance_key, element_instance_key);
            let var_events =
                self.propagated_updates(instance_key, flow_scope, pending.variables, false);
            // Insert variable merges just before the completion event so the
            // completing token sees them.
            let insert_at = events
                .iter()
                .position(|e| matches!(e, Event::UserTaskCompleted { .. }))
                .unwrap_or(events.len());
            for (offset, ev) in var_events.into_iter().enumerate() {
                events.insert(insert_at + offset, ev);
            }
        }

        (events, steps)
    }

    /// Applies the non-assignee corrections (candidates, dates, priority) of a
    /// task listener as a single `UserTaskUpdated`, if any are set. Returns an
    /// empty vec when there is nothing to update.
    fn apply_non_assignee_corrections(
        &self,
        user_task_key: Key,
        instance_key: Key,
        corrections: &crate::model::UserTaskCorrections,
    ) -> Vec<Event> {
        if corrections.candidate_groups.is_none()
            && corrections.candidate_users.is_none()
            && corrections.due_date.is_none()
            && corrections.follow_up_date.is_none()
            && corrections.priority.is_none()
        {
            return Vec::new();
        }
        vec![Event::UserTaskUpdated {
            user_task_key,
            instance_key,
            candidate_groups: corrections.candidate_groups.clone(),
            candidate_users: corrections.candidate_users.clone(),
            due_date: corrections
                .due_date
                .clone()
                .map(|d| if d.is_empty() { None } else { Some(d) }),
            follow_up_date: corrections.follow_up_date.clone().map(|d| {
                if d.is_empty() {
                    None
                } else {
                    Some(d)
                }
            }),
            priority: corrections.priority,
        }]
    }

    /// Applies a creating listener's corrections to a freshly-available user
    /// task. The assignee correction is honoured only when no initial assignee
    /// was declared (Zeebe parity, ADR 0037 §6).
    fn apply_creating_corrections(
        &self,
        user_task_key: Key,
        instance_key: Key,
        corrections: &crate::model::UserTaskCorrections,
        has_initial_assignee: bool,
    ) -> Vec<Event> {
        let mut events =
            self.apply_non_assignee_corrections(user_task_key, instance_key, corrections);
        if let Some(assignee) = &corrections.assignee {
            if !has_initial_assignee {
                events.push(Event::UserTaskAssigned {
                    user_task_key,
                    instance_key,
                    assignee: Some(assignee.clone()).filter(|a| !a.is_empty()),
                });
            }
        }
        events
    }

    /// Mints a fresh job for an already-active service-task element instance.
    /// Used by incident resolution to retry a parked service task: the element
    /// instance is left untouched (it stays active) and a new job is created in
    /// the `Created` (activatable) state so a worker can attempt it again. A
    /// no-op if the element is not (or is no longer) a service task.
    fn create_job_for(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        match self.element_kind(instance_key, &element_id) {
            Some(ElementKind::ServiceTask {
                job_type, priority, ..
            }) => {
                let job_key = self.mint_key();
                // The element instance is already active (this is an incident
                // retry), so resolve its FEEL attributes against its own applied
                // scope view (input mappings + ancestors).
                let job_vars = self.variables_for_element(instance_key, element_instance_key);
                let job_type = self.resolve_job_type(&job_vars, &job_type);
                let priority = self.resolve_priority(&job_vars, priority.as_deref());
                let retries = self.resolve_retries(
                    &job_vars,
                    self.retries_of(instance_key, &element_id).as_deref(),
                );
                (
                    vec![Event::JobCreated {
                        job_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        job_type,
                        created_at: self.now,
                        priority,
                        retries,
                    }],
                    Vec::new(),
                )
            }
            _ => (Vec::new(), Vec::new()),
        }
    }

    /// Exclusive gateway: take exactly one outgoing flow — the first whose
    /// condition holds (an unconditional flow is the default). If none qualifies,
    /// raise an incident and park the token.
    /// Selects the outgoing flow an exclusive gateway takes, evaluating each
    /// conditional flow in document order against the instance variables and
    /// falling back to the explicit `default` flow when none matches. Returns the
    /// chosen flow's index in the outgoing list (`Ok(Some)`), `Ok(None)` when nothing matches and
    /// there is no default (the caller raises a no-matching-flow incident), or
    /// `Err(reason)` when a condition failed to evaluate (an expression incident).
    /// Pure over the current variables, so the end-listener gate can re-select at
    /// [`finalize_exclusive_gateway`] time without captured state.
    fn select_exclusive_flow(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Result<Option<usize>, String> {
        let variables = self.variables(instance_key);
        let mut default_flow = None;
        for (index, flow) in self
            .outgoing(instance_key, element_id)
            .into_iter()
            .enumerate()
        {
            // The explicit `default` flow is a fallback only: it is never taken
            // by document order, but kept aside in case no conditional flow
            // matches.
            if flow.is_default {
                default_flow = Some(index);
                continue;
            }
            match &flow.condition {
                None => return Ok(Some(index)),
                Some(condition) => match condition.eval(&variables) {
                    Ok(true) => return Ok(Some(index)),
                    Ok(false) => continue,
                    Err(err) => {
                        return Err(format!(
                            "failed to evaluate condition '{}' at exclusive gateway \
                             '{element_id}': {}",
                            condition.expression, err.0
                        ));
                    }
                },
            }
        }
        Ok(default_flow)
    }

    fn complete_exclusive_gateway(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        let selected = match self.select_exclusive_flow(instance_key, &element_id) {
            Ok(sel) => sel,
            Err(reason) => {
                let incident_key = self.mint_key();
                return (
                    vec![Event::IncidentRaised {
                        incident_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        kind: state::IncidentKind::ExpressionEvaluation,
                        redrive: None,
                        reason,
                        job_key: None,
                        created_at: self.now,
                    }],
                    Vec::new(),
                );
            }
        };

        match selected {
            Some(flow_index) => {
                let scope = self.scope_of(instance_key, element_instance_key);
                let mut events = vec![Event::ElementCompleting {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                }];
                // End-listener gate (ADR 0037): the gateway rests in COMPLETING
                // while its end chain runs; `finalize_exclusive_gateway` re-selects
                // the flow and emits the deferred completion once it drains.
                let vars = self.variables(instance_key);
                if let Some(job) = self.begin_end_listener_chain(
                    instance_key,
                    element_instance_key,
                    &element_id,
                    scope,
                    &vars,
                ) {
                    events.push(job);
                    return (events, Vec::new());
                }
                events.push(Event::ElementCompleted {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                });
                let (taken, step) = self.take_flow(instance_key, &element_id, flow_index, scope);
                events.extend(taken);
                (events, vec![step])
            }
            None => {
                // The token stays active (parked on the incident) so the instance
                // does not falsely complete.
                let incident_key = self.mint_key();
                let events = vec![Event::IncidentRaised {
                    incident_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    kind: state::IncidentKind::NoMatchingSequenceFlow,
                    redrive: None,
                    reason: format!(
                        "no matching outgoing sequence flow at exclusive gateway '{element_id}'"
                    ),
                    job_key: None,
                    created_at: self.now,
                }];
                (events, Vec::new())
            }
        }
    }

    /// Deferred completion of an exclusive gateway whose `end` execution-listener
    /// chain has drained (ADR 0037). Re-selects the outgoing flow from the
    /// resident scope (the gateway rested in COMPLETING). Selection is normally
    /// deterministic, but a listener may have rewritten a condition variable; if
    /// re-selection now matches nothing (or a condition fails to evaluate) the
    /// gateway raises the same incident its non-listener path would and keeps the
    /// token active, rather than emitting `ElementCompleted` with nowhere to go
    /// (which would drop the token / falsely complete the instance).
    fn finalize_exclusive_gateway(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        match self.select_exclusive_flow(instance_key, &element_id) {
            Ok(Some(flow_index)) => {
                let (taken, step) = self.take_flow(instance_key, &element_id, flow_index, scope);
                let mut events = vec![Event::ElementCompleted {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                }];
                events.extend(taken);
                (events, vec![step])
            }
            Ok(None) => {
                // No flow matches now (and no default). Keep the token parked on
                // an incident — do not complete — mirroring the non-listener path.
                let incident_key = self.mint_key();
                (
                    vec![Event::IncidentRaised {
                        incident_key,
                        instance_key,
                        element_instance_key,
                        element_id: element_id.clone(),
                        kind: state::IncidentKind::NoMatchingSequenceFlow,
                        redrive: None,
                        reason: format!(
                            "no matching outgoing sequence flow at exclusive gateway '{element_id}'"
                        ),
                        job_key: None,
                        created_at: self.now,
                    }],
                    Vec::new(),
                )
            }
            Err(reason) => {
                let incident_key = self.mint_key();
                (
                    vec![Event::IncidentRaised {
                        incident_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        kind: state::IncidentKind::ExpressionEvaluation,
                        redrive: None,
                        reason,
                        job_key: None,
                        created_at: self.now,
                    }],
                    Vec::new(),
                )
            }
        }
    }

    /// Inclusive gateway: selects **every** outgoing flow whose condition holds
    /// (an unconditional non-default flow is always taken), falling back to the
    /// explicit `default` flow only when no conditional flow matches. Returns the
    /// chosen flows' indices in the outgoing list (`Ok(vec)`; an empty vec means nothing matched and there is
    /// no default — the caller raises a no-matching-flow incident), or
    /// `Err(reason)` when a condition failed to evaluate. Pure over the current
    /// variables so the end-listener gate can re-select without captured state.
    fn select_inclusive_flows(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Result<Vec<usize>, String> {
        let variables = self.variables(instance_key);
        let mut default_flow = None;
        let mut selected = Vec::new();
        for (index, flow) in self
            .outgoing(instance_key, element_id)
            .into_iter()
            .enumerate()
        {
            if flow.is_default {
                default_flow = Some(index);
                continue;
            }
            match &flow.condition {
                None => selected.push(index),
                Some(condition) => match condition.eval(&variables) {
                    Ok(true) => selected.push(index),
                    Ok(false) => continue,
                    Err(err) => {
                        return Err(format!(
                            "failed to evaluate condition '{}' at inclusive gateway \
                             '{element_id}': {}",
                            condition.expression, err.0
                        ));
                    }
                },
            }
        }
        if selected.is_empty() {
            if let Some(d) = default_flow {
                selected.push(d);
            }
        }
        Ok(selected)
    }

    /// Completes an inclusive gateway re-driven through `Step::Complete`: a
    /// single-incoming split, or a gateway whose incident was resolved. A join
    /// parked by a build older than #1241 has not consumed its tokens yet, so it
    /// consumes them first ([`Engine::fire_open_inclusive_join`]).
    fn complete_inclusive_gateway(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
    ) -> (Vec<Event>, Vec<Step>) {
        let scope = self.scope_of(instance_key, element_instance_key);
        let mut events = Vec::new();
        self.fire_open_inclusive_join(
            instance_key,
            element_instance_key,
            &element_id,
            scope,
            &mut events,
        );
        let (routed, followups) =
            self.route_inclusive_gateway(instance_key, element_instance_key, element_id, scope);
        events.extend(routed);
        (events, followups)
    }

    /// Routes an activated inclusive gateway (split or accepted join): takes
    /// every selected outgoing flow, gating the completion behind the `end`
    /// execution-listener chain (ADR 0037). A condition that fails to evaluate
    /// raises an `ExpressionEvaluation` incident; a selection that matches no
    /// flow (and no default) raises a `NoMatchingSequenceFlow` incident and
    /// parks the token, mirroring the exclusive gateway and Zeebe's
    /// `InclusiveGatewayProcessor.finalizeActivation`.
    fn route_inclusive_gateway(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let selected = match self.select_inclusive_flows(instance_key, &element_id) {
            Ok(sel) => sel,
            Err(reason) => {
                let incident_key = self.mint_key();
                return (
                    vec![Event::IncidentRaised {
                        incident_key,
                        instance_key,
                        element_instance_key,
                        element_id,
                        kind: state::IncidentKind::ExpressionEvaluation,
                        redrive: None,
                        reason,
                        job_key: None,
                        created_at: self.now,
                    }],
                    Vec::new(),
                );
            }
        };
        if selected.is_empty() {
            let incident_key = self.mint_key();
            return (
                vec![Event::IncidentRaised {
                    incident_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    kind: state::IncidentKind::NoMatchingSequenceFlow,
                    redrive: None,
                    reason: format!(
                        "no matching outgoing sequence flow at inclusive gateway '{element_id}'"
                    ),
                    job_key: None,
                    created_at: self.now,
                }],
                Vec::new(),
            );
        }
        let mut events = vec![Event::ElementCompleting {
            instance_key,
            element_instance_key,
            element_id: element_id.clone(),
        }];
        let vars = self.variables(instance_key);
        if let Some(job) = self.begin_end_listener_chain(
            instance_key,
            element_instance_key,
            &element_id,
            scope,
            &vars,
        ) {
            events.push(job);
            return (events, Vec::new());
        }
        events.push(Event::ElementCompleted {
            instance_key,
            element_instance_key,
            element_id: element_id.clone(),
        });
        let mut followups = Vec::new();
        for flow_index in selected {
            let (taken, step) = self.take_flow(instance_key, &element_id, flow_index, scope);
            events.extend(taken);
            followups.push(step);
        }
        (events, followups)
    }

    /// Deferred completion of an inclusive-gateway split whose `end`
    /// execution-listener chain has drained (ADR 0037). Re-selects from the
    /// resident scope; a listener may have rewritten a condition variable, so a
    /// re-selection that now matches nothing (or fails to evaluate) parks the
    /// token on the same incident the non-listener path would raise rather than
    /// completing with nowhere to go.
    fn finalize_inclusive_gateway(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: String,
        scope: Key,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut events = Vec::new();
        self.fire_open_inclusive_join(
            instance_key,
            element_instance_key,
            &element_id,
            scope,
            &mut events,
        );
        match self.select_inclusive_flows(instance_key, &element_id) {
            Ok(selected) if !selected.is_empty() => {
                events.push(Event::ElementCompleted {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                });
                let mut followups = Vec::new();
                for flow_index in selected {
                    let (taken, step) =
                        self.take_flow(instance_key, &element_id, flow_index, scope);
                    events.extend(taken);
                    followups.push(step);
                }
                (events, followups)
            }
            Ok(_) => {
                let incident_key = self.mint_key();
                events.push(Event::IncidentRaised {
                    incident_key,
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                    kind: state::IncidentKind::NoMatchingSequenceFlow,
                    redrive: None,
                    reason: format!(
                        "no matching outgoing sequence flow at inclusive gateway '{element_id}'"
                    ),
                    job_key: None,
                    created_at: self.now,
                });
                (events, Vec::new())
            }
            Err(reason) => {
                let incident_key = self.mint_key();
                events.push(Event::IncidentRaised {
                    incident_key,
                    instance_key,
                    element_instance_key,
                    element_id,
                    kind: state::IncidentKind::ExpressionEvaluation,
                    redrive: None,
                    reason,
                    job_key: None,
                    created_at: self.now,
                });
                (events, Vec::new())
            }
        }
    }

    /// If `element_instance_key` is still the *open* join for `element_id`,
    /// append its firing to `events`: [`Event::ParallelJoinFired`] consumes one
    /// token per incoming flow, and a surplus reopens the join
    /// ([`Engine::fire_join`]). Since #1241 an accepted join consumes when it
    /// activates ([`Engine::activate_join`]), so its instance is no longer the
    /// open join and this is a no-op. It still fires a join that a build older
    /// than #1241 parked on an incident or an `end` listener chain before
    /// consuming, when that join's completion is re-driven from a restored
    /// snapshot or a replayed journal. It is also a no-op for a single-incoming
    /// inclusive split (no open join exists).
    fn fire_open_inclusive_join(
        &mut self,
        instance_key: Key,
        element_instance_key: Key,
        element_id: &str,
        scope: Key,
        events: &mut Vec<Event>,
    ) {
        if self.join_eik(instance_key, element_id) == Some(element_instance_key) {
            let arrivals = self.join_arrivals(instance_key, element_id);
            self.fire_join(instance_key, element_id, scope, arrivals, events);
        }
    }

    /// Which join `element_id` is, if any: a parallel or inclusive gateway with
    /// more than one incoming flow.
    fn join_kind(&self, instance_key: Key, element_id: &str) -> Option<JoinKind> {
        let join = match self
            .process_of_instance(instance_key)?
            .element(element_id)?
            .kind
        {
            ElementKind::ParallelGateway => JoinKind::Parallel,
            ElementKind::InclusiveGateway => JoinKind::Inclusive,
            _ => return None,
        };
        (self.incoming_count(instance_key, element_id) > 1).then_some(join)
    }

    /// Take `from`'s `index`-th outgoing sequence flow: the
    /// [`Event::SequenceFlowTaken`] fact plus the activation of its target,
    /// carrying the flow's [`IncomingFlow`] identity. A flow into a join is
    /// counted onto the join here, when it is taken, as Zeebe's
    /// `ProcessInstanceSequenceFlowTakenApplier` does, so the join's guard sees
    /// every flow taken before its activation is processed (#1233, #1241).
    /// Every sequence flow the engine takes is built here, so no taker can drop
    /// the identity or the count.
    fn take_flow(
        &self,
        instance_key: Key,
        from: &str,
        index: usize,
        scope: Key,
    ) -> (Vec<Event>, Step) {
        let (to, flow) = self
            .process_of_instance(instance_key)
            .and_then(|p| p.incoming_flow(from, index))
            .expect("a taken flow is an outgoing flow of `from` in the instance's definition");
        let mut events = vec![Event::SequenceFlowTaken {
            instance_key,
            from: from.to_string(),
            to: to.clone(),
        }];
        if self.join_kind(instance_key, &to).is_some() {
            events.push(Event::ParallelJoinTokenArrived {
                instance_key,
                element_id: to.clone(),
                flow: Some(flow.clone()),
            });
        }
        (
            events,
            Step::Activate {
                instance_key,
                element_id: to,
                scope,
                via: Some(flow),
            },
        )
    }

    /// [`Engine::take_flow`] for every outgoing flow of `from`, in document
    /// order (a pass-through element or a parallel split).
    fn take_all_flows(&self, instance_key: Key, from: &str, scope: Key) -> Vec<(Vec<Event>, Step)> {
        (0..self.outgoing(instance_key, from).len())
            .map(|index| self.take_flow(instance_key, from, index, scope))
            .collect()
    }

    /// A join's activation: Zeebe's `ACTIVATE_ELEMENT` for a parallel or
    /// inclusive gateway, guarded as in `ProcessInstanceStateTransitionGuard`
    /// (`canActivateParallelGateway` / `canActivateInclusiveGateway`). A flow's
    /// token was counted when the flow was taken ([`Engine::take_flow`]); an
    /// activation that did not come over a flow is counted here, unidentified.
    ///
    /// - A parallel join is accepted once every incoming flow has been taken.
    /// - An inclusive join is accepted once every incoming flow has been taken,
    ///   or once at least one has and no live path can still reach it
    ///   ([`Engine::has_active_path_to`]).
    ///
    /// An accepted join activates, consumes one token per taken flow and keeps
    /// the surplus (Zeebe's "Tetris principle",
    /// `ProcessInstanceElementActivatingV3Applier`), then completes and routes.
    /// A rejected activation changes nothing; tokens still counted wait on an
    /// open join instance, which keeps the process instance alive as Zeebe's
    /// active-sequence-flow count does. The guard runs only here, so a join is
    /// re-evaluated only when another activation reaches it: a surplus nothing
    /// follows, or a competing path that ends elsewhere, strands the waiting
    /// tokens exactly as in Zeebe.
    fn activate_join(
        &mut self,
        instance_key: Key,
        element_id: String,
        scope: Key,
        via: Option<IncomingFlow>,
        join: JoinKind,
        pending: &VecDeque<Step>,
    ) -> (Vec<Event>, Vec<Step>) {
        let mut events = Vec::new();
        let mut unidentified = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.join_counts.get(&element_id).copied())
            .unwrap_or(0);
        if via.is_none() {
            events.push(Event::ParallelJoinTokenArrived {
                instance_key,
                element_id: element_id.clone(),
                flow: None,
            });
            unidentified += 1;
        }
        let arrivals = self.join_arrivals(instance_key, &element_id);
        let taken = arrivals.distinct_flows() + unidentified;
        let all_taken = taken >= self.incoming_count(instance_key, &element_id);
        let accepted = match join {
            JoinKind::Parallel => all_taken,
            JoinKind::Inclusive => {
                all_taken
                    || (taken > 0
                        && !self.has_active_path_to(
                            instance_key,
                            &element_id,
                            scope,
                            &arrivals,
                            pending,
                        ))
            }
        };

        let open = self.join_eik(instance_key, &element_id);
        if !accepted {
            if taken > 0 && open.is_none() {
                self.open_join(instance_key, &element_id, scope, &mut events);
            }
            return (events, Vec::new());
        }

        // The accepted activation takes over the join instance holding the
        // waiting tokens, or activates a fresh one if none waited.
        let element_instance_key = match open {
            Some(eik) => eik,
            None => {
                let eik = self.mint_key();
                events.push(Event::ElementActivating {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                });
                events.push(Event::ElementActivated {
                    instance_key,
                    element_instance_key: eik,
                    element_id: element_id.clone(),
                    scope,
                });
                eik
            }
        };
        self.fire_join(instance_key, &element_id, scope, arrivals, &mut events);

        let followups = match join {
            JoinKind::Parallel => {
                events.push(Event::ElementCompleting {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                });
                events.push(Event::ElementCompleted {
                    instance_key,
                    element_instance_key,
                    element_id: element_id.clone(),
                });
                let mut followups = Vec::new();
                for (taken, step) in self.take_all_flows(instance_key, &element_id, scope) {
                    events.extend(taken);
                    followups.push(step);
                }
                followups
            }
            JoinKind::Inclusive => {
                let (routed, followups) = self.route_inclusive_gateway(
                    instance_key,
                    element_instance_key,
                    element_id,
                    scope,
                );
                events.extend(routed);
                followups
            }
        };
        (events, followups)
    }

    /// Zeebe's `BpmnInclusiveGatewayBehavior.hasActivePathToTheGateway`: can a
    /// live token in the join's flow scope still reach it? The sources are the
    /// scope's active element instances other than the join's own, plus the
    /// targets of flows taken but not yet activated (Zeebe's active sequence
    /// flows; here the flow activations still queued in `pending`). From each,
    /// the search follows outgoing flows, an activity's boundary events and a
    /// link throw's catch, and skips the join's incoming flows already in
    /// `taken`: every incoming flow needs to be taken only once.
    fn has_active_path_to(
        &self,
        instance_key: Key,
        join: &str,
        scope: Key,
        taken: &state::FlowArrivals,
        pending: &VecDeque<Step>,
    ) -> bool {
        let (Some(process), Some(instance)) = (
            self.process_of_instance(instance_key),
            self.state.instances.get(&instance_key),
        ) else {
            return false;
        };
        let active = instance
            .active
            .iter()
            .filter(|(eik, id)| id.as_str() != join && self.scope_of(instance_key, **eik) == scope)
            .map(|(_, id)| id.as_str());
        let in_transit = pending.iter().filter_map(|step| match step {
            Step::Activate {
                instance_key: key,
                element_id,
                scope: step_scope,
                via: Some(_),
            } if *key == instance_key && *step_scope == scope && element_id != join => {
                Some(element_id.as_str())
            }
            _ => None,
        });
        active
            .chain(in_transit)
            .any(|source| path_reaches_join(process, source, join, taken))
    }

    /// A join fires: [`Event::ParallelJoinFired`] consumes one token per incoming
    /// flow from `arrivals` (the join's per-flow tokens as they stand once the
    /// firing arrival is counted) and keeps the surplus, Zeebe's "Tetris
    /// principle". A surplus is still a live token, so it reopens the join on a
    /// fresh element instance, which keeps the instance alive. Shared by
    /// parallel and inclusive joins (#1233, #1237).
    fn fire_join(
        &mut self,
        instance_key: Key,
        element_id: &str,
        scope: Key,
        mut arrivals: state::FlowArrivals,
        events: &mut Vec<Event>,
    ) {
        events.push(Event::ParallelJoinFired {
            instance_key,
            element_id: element_id.to_string(),
        });
        arrivals.consume_one_each();
        if !arrivals.is_empty() {
            self.open_join(instance_key, element_id, scope, events);
        }
    }

    /// The per-flow tokens currently waiting on `element_id`'s open join.
    fn join_arrivals(&self, instance_key: Key, element_id: &str) -> state::FlowArrivals {
        self.state
            .instances
            .get(&instance_key)
            .and_then(|i| i.join_flow_arrivals.get(element_id))
            .cloned()
            .unwrap_or_default()
    }

    /// Open a join (parallel or inclusive): a fresh element instance that holds
    /// its waiting tokens. Returns its key.
    fn open_join(
        &mut self,
        instance_key: Key,
        element_id: &str,
        scope: Key,
        events: &mut Vec<Event>,
    ) -> Key {
        let eik = self.mint_key();
        events.push(Event::ElementActivating {
            instance_key,
            element_instance_key: eik,
            element_id: element_id.to_string(),
        });
        events.push(Event::ElementActivated {
            instance_key,
            element_instance_key: eik,
            element_id: element_id.to_string(),
            scope,
        });
        events.push(Event::ParallelJoinOpened {
            instance_key,
            element_instance_key: eik,
            element_id: element_id.to_string(),
        });
        eik
    }

    /// After a command settles, any active instance with no remaining tokens has
    /// completed. Returns any follow-up steps that must run because of a
    /// completion — currently the [`Step::CompleteCallActivity`] that releases a
    /// parent's parked call-activity token when its spawned child finishes.
    fn complete_finished_instances(&mut self, log: &mut Vec<Event>) -> Vec<Step> {
        let touched: HashSet<Key> = log.iter().filter_map(|e| e.instance_key()).collect();
        let mut finished: Vec<Key> = touched
            .into_iter()
            .filter(|k| {
                self.state
                    .instances
                    .get(k)
                    .map(|i| i.state == ProcessInstanceState::Active && i.active.is_empty())
                    .unwrap_or(false)
            })
            .collect();
        // Deterministic completion order (a set iteration is unordered).
        finished.sort_unstable();

        let mut followups = Vec::new();
        for instance_key in finished {
            // A completing call-activity child releases its parent's parked token.
            // Captured before `ProcessInstanceCompleted` drops the child's
            // variables so the call activity's output mappings still see them.
            if let Some(step) = self.call_activity_completion_step(instance_key) {
                followups.push(step);
            }
            self.emit(log, Event::ProcessInstanceCompleted { instance_key });
        }
        followups
    }

    /// Re-attempts the child-process spawn for a `callActivity` ad-hoc tool
    /// (issue #1159) whose first spawn parked a recoverable incident (bad
    /// `=calledElement` expression, unknown callee, or depth overflow). The tool
    /// child element instance is still ACTIVATED in the container's active set and
    /// its local scope already carries the agent seed + the tool's applied input
    /// mappings from the first pass, so this re-derives the callee id and
    /// `propagateAllParentVariables` from the container catalog and re-runs the
    /// shared spawn. Creating the child on success routes its completion back
    /// through `complete_adhoc_call_activity_tool` exactly as a first-pass spawn
    /// would.
    ///
    /// `preserved_seed` (#1176) is the tool's single-pass input projection
    /// captured on the failed first spawn: the tool child's local scope already
    /// carries the first pass's applied input targets, so re-projecting the
    /// (single-pass, possibly chained) input mappings here would read a mutated
    /// view and silently alter them (`x -> y`, `y -> z`: the retry would resolve
    /// `z` non-null). It seeds the child by Zeebe parity
    /// (`CallActivityProcessor.finalizeActivation`): under
    /// `propagateAllParentVariables=true` the preserved projection is overlaid
    /// onto a FRESHLY re-read all-parent view (Zeebe's
    /// `copyAllVariablesToProcessInstance` re-reads the call activity's current
    /// scope on redrive), so container variables that became visible after the
    /// incident still propagate into the child; with it off, only the preserved
    /// projection crosses (`copyLocalVariablesToProcessInstance`). The activating
    /// `view` is re-read fresh regardless so the `=calledElement` expression
    /// re-evaluates against the operator's fix. `preserved_seed` is `None` only
    /// for a legacy incident predating #1176 (a plain `CallActivitySpawn` redrive
    /// on an ad-hoc tool), which re-projects the input mappings instead.
    fn respawn_adhoc_call_activity_tool(
        &mut self,
        instance_key: Key,
        child_eik: Key,
        container_key: Key,
        element_id: String,
        preserved_seed: Option<HashMap<String, Value>>,
    ) -> (Vec<Event>, Vec<Step>) {
        let container_element_id = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
        {
            Some(a) => a.element_id.clone(),
            None => return (Vec::new(), Vec::new()),
        };
        let (process_id, propagate_all_parent) = match self
            .adhoc_def_of(instance_key, &container_element_id)
            .and_then(|def| {
                def.tools
                    .iter()
                    .find(|t| t.element_id == element_id)
                    .map(|t| t.kind.clone())
            }) {
            Some(crate::model::AdHocToolKind::CallActivity {
                process_id: Some(process_id),
                propagate_all_parent_variables,
                ..
            }) => (process_id, propagate_all_parent_variables),
            // An unbound call-activity tool (`process_id: None`) never spawns (it
            // passes straight through on activation), so no spawn incident can
            // exist to re-drive here — return a no-op rather than re-attempting a
            // spawn with an empty callee.
            _ => return (Vec::new(), Vec::new()),
        };
        // Re-read the activating view fresh so the `=calledElement` expression
        // re-evaluates against the operator's fix. The child *seed* does NOT come
        // from re-projecting inputs against this (already-mutated) view — see
        // `preserved_seed` below.
        let view = (*self.variables_for_element(instance_key, child_eik)).clone();
        // The single-pass input-mapping projection: preserved verbatim from the
        // failed first spawn (#1176) so a chained input mapping is not re-applied
        // against the already-mutated child scope. For a legacy pre-#1176 incident
        // (plain `CallActivitySpawn` redrive on an ad-hoc tool) it is re-projected
        // — idempotent for non-chained inputs; the preservation path is what fixes
        // the chained case.
        let projection = match preserved_seed {
            Some(seed) => seed,
            None => match self.adhoc_tool_input_updates(
                instance_key,
                &container_element_id,
                &element_id,
                &view,
            ) {
                Ok(updates) => updates,
                Err(failure) => {
                    let event = self.io_mapping_incident(
                        instance_key,
                        child_eik,
                        element_id,
                        failure,
                        state::IoMappingRedrive::CallActivitySpawn,
                    );
                    return (vec![event], Vec::new());
                }
            },
        };
        // Zeebe parity (`CallActivityProcessor.finalizeActivation`): when the spawn
        // incident resolves, `copyAllVariablesToProcessInstance` re-reads the call
        // activity's CURRENT scope, so `propagateAllParentVariables=true` seeds the
        // child from a FRESH all-parent view with the preserved input projection
        // layered on top (child-local wins) — picking up container variables that
        // became visible after the incident. With it off, only the preserved input
        // projection crosses (`copyLocalVariablesToProcessInstance`).
        let child_seed = if propagate_all_parent {
            let mut merged = view.clone();
            merged.extend(projection.clone());
            merged
        } else {
            projection.clone()
        };
        self.spawn_call_activity_instance(
            instance_key,
            child_eik,
            &element_id,
            &process_id,
            &view,
            child_seed,
            Some(projection),
        )
    }

    /// Resolves the ad-hoc container (and the tool's inner-instance wrapper) that
    /// owns `element_instance_key` when it is an ad-hoc tool child, or `None`
    /// otherwise (issue #1159). An ad-hoc tool child hangs off an
    /// `AD_HOC_SUB_PROCESS_INNER_INSTANCE` whose own scope is the container that
    /// still lists the tool child in its `active` set — the exact scope-chain
    /// check `complete` uses to route a completing tool child to
    /// `complete_adhoc_tool`. Used by `complete_call_activity` to recognise a
    /// call-activity tool whose spawned child has just finished.
    fn adhoc_tool_container_of(
        &self,
        instance_key: Key,
        element_instance_key: Key,
    ) -> Option<(Key, Key)> {
        let inner_key = self.scope_of(instance_key, element_instance_key);
        if inner_key == 0 {
            return None;
        }
        let container_key = self.scope_of(instance_key, inner_key);
        if container_key == 0 {
            return None;
        }
        let is_tool = self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
            .map(|a| a.active.contains(&element_instance_key))
            .unwrap_or(false);
        is_tool.then_some((container_key, inner_key))
    }

    /// Completes a `callActivity` ad-hoc tool (issue #1159) once its spawned
    /// child process instance finished. Unlike a service-task tool — whose result
    /// variables are written into the tool child's own scope by the agent's job
    /// completion — a call-activity tool's results live in a *separate* child
    /// instance and cross back through the tool's output mapping (and, with
    /// `propagateAllChildVariables`, the whole child scope). This bridges the
    /// gap: it evaluates the tool's `zeebe:output` mappings against what the child
    /// actually produced (`child_variables`) and writes both those projected
    /// results and — when propagation is on — the raw child variables into the
    /// tool child's LOCAL scope, then hands off to the shared
    /// `complete_adhoc_tool` leaf path (via `Step::Complete`). That way the
    /// container's `outputElement` sees the real projected result before it is
    /// collected, and the tool's output mapping projects into the container scope
    /// exactly as for any other tool — no more all-null manufactured answer. The
    /// tool child's local scope is torn down on completion, so the raw child
    /// variables never leak past the intended projections when propagation is
    /// off.
    fn complete_adhoc_call_activity_tool(
        &mut self,
        instance_key: Key,
        child_eik: Key,
        element_id: String,
        container_key: Key,
        inner_key: Key,
        child_variables: HashMap<String, Value>,
    ) -> (Vec<Event>, Vec<Step>) {
        let container_element_id = match self
            .state
            .instances
            .get(&instance_key)
            .and_then(|i| i.adhoc_instances.get(&container_key))
        {
            Some(a) => a.element_id.clone(),
            None => return (Vec::new(), Vec::new()),
        };
        // Honour `propagateAllChildVariables` (Zeebe default `true`) as declared
        // on the tool's catalog entry.
        let propagate_all_child = self
            .adhoc_def_of(instance_key, &container_element_id)
            .and_then(|def| {
                def.tools
                    .iter()
                    .find(|t| t.element_id == element_id)
                    .map(|t| match &t.kind {
                        crate::model::AdHocToolKind::CallActivity {
                            propagate_all_child_variables,
                            ..
                        } => *propagate_all_child_variables,
                        _ => true,
                    })
            })
            .unwrap_or(true);
        // Project the tool's output mapping against the child's produced
        // variables (NOT the tool child's scope, which the child never wrote) —
        // the fix for the all-null result. A failing mapping halts the tool with
        // an incident, mirroring `complete_adhoc_tool`; resolution re-drives the
        // completion (the child that produced these variables is already gone, so
        // they are captured on the incident's redrive).
        let outputs = self
            .adhoc_tool_io(instance_key, &container_element_id, &element_id)
            .outputs;
        let output_updates = if outputs.is_empty() {
            HashMap::new()
        } else {
            match self.eval_io_mappings_in(&child_variables, &outputs) {
                Ok(updates) => updates,
                Err(failure) => {
                    // Preserve the child's produced variables on the incident's
                    // redrive (issue #1159): the child process instance that
                    // produced them has already completed and is gone by
                    // resolution time, so a generic `Completion` redrive (which
                    // re-drives `Step::Complete` into `complete_adhoc_tool`
                    // against the pre-child tool scope) could never re-project
                    // them. `CallActivityCompletion` re-drives
                    // `Step::CompleteCallActivity`, which re-enters
                    // `complete_call_activity` → this same bridge with the
                    // captured `child_variables`, so the now-fixed output mapping
                    // re-projects the real result.
                    let event = self.io_mapping_incident(
                        instance_key,
                        child_eik,
                        element_id,
                        failure,
                        state::IoMappingRedrive::CallActivityCompletion {
                            child_variables: child_variables.clone(),
                        },
                    );
                    return (vec![event], Vec::new());
                }
            }
        };
        // Seed the tool child's LOCAL scope with the raw child variables AND the
        // projected output. The shared leaf path (`complete_adhoc_tool`) evaluates
        // the container's `outputElement` against this scope, so it must resolve
        // the output target (e.g. `outputElement="=toolCallResult"`) and every
        // field it references (e.g. `status`/`summary`). This scope is torn down
        // when the tool child completes, so the raw child variables never leak
        // past the intended projections regardless of `propagateAllChildVariables`.
        let mut seed = child_variables.clone();
        seed.extend(output_updates.clone());
        let mut events = Vec::new();
        if !seed.is_empty() {
            events.push(Event::ScopedVariablesUpdated {
                instance_key,
                scope_key: child_eik,
                variables: seed,
            });
        }
        // `propagateAllChildVariables` (Zeebe default `true`): when on, merge the
        // child's raw variables into the CONTAINER scope — the tool's parent —
        // mirroring the sequence-flow call-activity path. The leaf projects the
        // tool's output mapping into the container AFTER this, so an explicit
        // output mapping still overrides a propagated value. When off, only the
        // output mapping crosses back.
        if propagate_all_child && !child_variables.is_empty() {
            events.extend(self.propagated_updates(
                instance_key,
                container_key,
                child_variables,
                false,
            ));
        }
        // Hand off to the shared ad-hoc tool completion leaf path, passing the
        // output projection computed ABOVE so it is applied verbatim rather than
        // re-evaluated against the seeded scope (avoiding the double-projection of
        // chained mappings — issue #1159). The seed events above are applied
        // before this step runs, so the leaf's `outputElement` evaluation sees the
        // real result.
        let followups = vec![Step::CompleteAdHocCallActivityTool {
            instance_key,
            element_instance_key: child_eik,
            element_id,
            container_key,
            inner_key,
            output_updates,
        }];
        (events, followups)
    }

    /// the current command that leaves its parent's call-activity token behind
    /// (Zeebe parity: cancelling the parent cancels the child), transitively down
    /// the parent-child tree. Called after the command drains, so it sees every
    /// terminal instance record the command produced. Two events seed the sweep:
    ///
    /// * `ProcessInstanceTerminated` — a direct `CancelInstance`, a scope
    ///   interruption, or a cascaded parent cancel.
    /// * `ProcessInstanceCompleted` — normally a completion has no live children
    ///   (an instance only completes with an empty `active` map, so no
    ///   call-activity token is parked), making the scan a no-op; the exception
    ///   is a **top-level terminate end event** (#1085), which completes the
    ///   instance while its sibling call-activity children are still live and
    ///   must be terminated here.
    fn cascade_cancel_children(&mut self, log: &mut Vec<Event>) {
        let mut ended: HashSet<Key> = log
            .iter()
            .filter_map(|e| match e {
                Event::ProcessInstanceTerminated { instance_key }
                | Event::ProcessInstanceCompleted { instance_key } => Some(*instance_key),
                _ => None,
            })
            .collect();
        if ended.is_empty() {
            return;
        }
        loop {
            let mut children: Vec<Key> = self
                .state
                .instances
                .values()
                .filter(|i| {
                    // A cascade cancel is not interruptible, so it must also
                    // sweep children already mid-drain (`Terminating`, e.g. a
                    // child whose own cancel deferred on a user-task canceling
                    // listener) — otherwise the parent's termination orphans
                    // them. `discard_and_terminate_instance` force-completes
                    // that deferred drain.
                    matches!(
                        i.state,
                        ProcessInstanceState::Active | ProcessInstanceState::Terminating
                    ) && i
                        .parent_process_instance_key
                        .map(|p| ended.contains(&p))
                        .unwrap_or(false)
                })
                .map(|i| i.key)
                .collect();
            if children.is_empty() {
                break;
            }
            children.sort_unstable();
            for child in children {
                self.discard_and_terminate_instance(log, child);
                ended.insert(child);
            }
        }
    }

    /// Immediately discards every token of `instance_key` (its in-play jobs,
    /// armed timers, open subscriptions and user tasks) and terminates it. This
    /// is the forced-cancel path used to cascade a parent cancellation to a
    /// call-activity child — unlike [`Command::CancelInstance`] it never defers on
    /// user-task `canceling` listeners (a cascade cancel is not interruptible).
    fn discard_and_terminate_instance(&mut self, log: &mut Vec<Event>, instance_key: Key) {
        for event in self.discard_instance_events(instance_key) {
            self.emit(log, event);
        }
    }

    /// The events that discard and terminate a whole process instance: the
    /// cancellations of every in-play job, armed timer, open message/signal/
    /// conditional subscription and created user task on the instance, followed
    /// by `ProcessInstanceTerminated` (whose reducer clears the remaining active
    /// tokens and scopes). Returns the events (does not emit) so it composes
    /// inside an emit-driven command tail ([`discard_and_terminate_instance`]).
    fn discard_instance_events(&self, instance_key: Key) -> Vec<Event> {
        let mut cancels = self.discard_instance_token_events(instance_key);
        cancels.push(Event::ProcessInstanceTerminated { instance_key });
        cancels
    }

    /// The cancellations of every in-play token of `instance_key` — its created/
    /// activated/failed jobs, armed timers, open message/signal/conditional
    /// subscriptions and created user tasks — **without** the terminal instance
    /// record. The caller appends the terminal event: `ProcessInstanceTerminated`
    /// for a cancel/cascade teardown ([`discard_instance_events`]), or
    /// `ProcessInstanceCompleted` for a top-level terminate end event (Zeebe
    /// parity: the terminate end kills every inner token yet the process instance
    /// itself ends COMPLETED — see [`complete_terminate_end`]). Returns the events
    /// (does not emit) so it composes inside a decide-only `process_step` result.
    fn discard_instance_token_events(&self, instance_key: Key) -> Vec<Event> {
        let mut cancels: Vec<Event> = Vec::new();

        let mut jobs: Vec<&state::Job> = self
            .state
            .jobs
            .values()
            .filter(|j| {
                j.instance_key == instance_key
                    && matches!(
                        j.state,
                        state::JobState::Created
                            | state::JobState::Activated
                            | state::JobState::Failed
                    )
            })
            .collect();
        jobs.sort_unstable_by_key(|j| j.key);
        cancels.extend(jobs.iter().map(|j| Event::JobCanceled {
            job_key: j.key,
            instance_key,
        }));

        let mut timers: Vec<&state::Timer> = self
            .state
            .timers
            .values()
            .filter(|t| t.instance_key == instance_key && t.state == state::TimerState::Created)
            .collect();
        timers.sort_unstable_by_key(|t| t.key);
        cancels.extend(timers.iter().map(|t| Event::TimerCanceled {
            timer_key: t.key,
            instance_key,
            element_instance_key: t.element_instance_key,
            element_id: t.element_id.clone(),
        }));

        let mut subs: Vec<&state::MessageSubscription> = self
            .state
            .message_subscriptions
            .values()
            .filter(|s| {
                s.instance_key == instance_key
                    && matches!(
                        s.state,
                        state::MessageSubscriptionState::Open
                            | state::MessageSubscriptionState::Opening
                    )
            })
            .collect();
        subs.sort_unstable_by_key(|s| s.key);
        cancels.extend(subs.iter().map(|s| Self::disarm_subscription_event(s)));

        let mut sig_subs: Vec<&state::SignalSubscription> = self
            .state
            .signal_subscriptions
            .values()
            .filter(|s| {
                s.instance_key == instance_key && s.state == state::MessageSubscriptionState::Open
            })
            .collect();
        sig_subs.sort_unstable_by_key(|s| s.key);
        cancels.extend(sig_subs.iter().map(|s| Event::SignalSubscriptionCanceled {
            subscription_key: s.key,
            instance_key,
            element_instance_key: s.element_instance_key,
            element_id: s.element_id.clone(),
        }));

        let mut cond_subs: Vec<&state::ConditionalSubscription> = self
            .state
            .conditional_subscriptions
            .values()
            .filter(|s| {
                s.instance_key == instance_key && s.state == state::MessageSubscriptionState::Open
            })
            .collect();
        cond_subs.sort_unstable_by_key(|s| s.key);
        cancels.extend(
            cond_subs
                .iter()
                .map(|s| Event::ConditionalSubscriptionCanceled {
                    subscription_key: s.key,
                    instance_key,
                    element_instance_key: s.element_instance_key,
                    element_id: s.element_id.clone(),
                }),
        );

        let mut user_tasks: Vec<&state::UserTask> = self
            .state
            .user_tasks
            .values()
            .filter(|t| t.instance_key == instance_key && t.state == state::UserTaskState::Created)
            .collect();
        user_tasks.sort_unstable_by_key(|t| t.key);
        cancels.extend(user_tasks.iter().map(|t| Event::UserTaskCanceled {
            user_task_key: t.key,
            instance_key,
        }));

        cancels
    }

    /// `IncidentResolved` events for every incident still `Active` anywhere on
    /// `instance_key` (deterministic by incident key). Used by the top-level
    /// terminate end event, which completes the instance via
    /// `ProcessInstanceCompleted` — whose reducer, unlike `ProcessInstanceTerminated`,
    /// deliberately leaves incident records untouched — so the terminate end must
    /// close its own open incidents. The caller emits these **after** the token
    /// cancellations ([`discard_instance_token_events`]) so a failed job's
    /// `JobCanceled` precedes its `IncidentResolved`; the reducer's job-resurrection
    /// guard then leaves the cancelled job terminal. Returns the events (does not
    /// emit) so it composes inside a decide-only `process_step` result.
    fn resolve_instance_incidents(&self, instance_key: Key) -> Vec<Event> {
        // Use the instance's own `incidents` index (keys of its currently active
        // incidents) rather than scanning the global `state.incidents` map — that
        // scan is O(total incidents) and grows with unrelated concurrent
        // instances. The index is kept to only open incidents by the reducer, so
        // the `Active` filter below is belt-and-braces; the sort keeps the
        // `IncidentResolved` order deterministic regardless of index order.
        let Some(instance) = self.state.instances.get(&instance_key) else {
            return Vec::new();
        };
        let mut incidents: Vec<&state::Incident> = instance
            .incidents
            .iter()
            .filter_map(|k| self.state.incidents.get(k))
            .filter(|i| i.state == state::IncidentState::Active)
            .collect();
        incidents.sort_unstable_by_key(|i| i.key);
        incidents
            .into_iter()
            .map(|i| Event::IncidentResolved {
                incident_key: i.key,
                instance_key,
                job_key: i.job_key,
                resolved_at: self.now,
                operation_reference: None,
            })
            .collect()
    }
    fn emit(&mut self, log: &mut Vec<Event>, event: Event) {
        let resolution = match &event {
            Event::JobCompleted {
                job_key,
                instance_key,
                ..
            } => Some((
                *instance_key,
                *job_key,
                self.state
                    .jobs
                    .get(job_key)
                    .and_then(|job| job.lease_token.clone())
                    .unwrap_or_default(),
                true,
            )),
            Event::JobCanceled {
                job_key,
                instance_key,
                ..
            }
            | Event::JobErrorThrown {
                job_key,
                instance_key,
                ..
            } => Some((*instance_key, *job_key, String::new(), false)),
            _ => None,
        };
        if let Event::ProcessInstanceCompleted { instance_key }
        | Event::ProcessInstanceTerminated { instance_key } = &event
        {
            self.cleanup_agent_instances(log, *instance_key);
        }
        if self.track_dirty_vars {
            match &event {
                Event::ProcessInstanceCreated { instance_key, .. }
                | Event::VariablesUpdated { instance_key, .. } => {
                    self.dirty_vars.insert(*instance_key);
                    self.forgotten_vars.remove(instance_key);
                }
                // A terminal instance drops its variables in `state::apply`
                // (ADR 0012). Mirror that in the durable store: forget the
                // payload so a snapshot taken before exporter-driven eviction
                // cannot resurrect it on recovery. Ordering is deliberate — this
                // wins over any dirty mark from the same command's earlier
                // `VariablesUpdated` (e.g. job-output merge then completion).
                Event::ProcessInstanceCompleted { instance_key }
                | Event::ProcessInstanceTerminated { instance_key } => {
                    self.dirty_vars.remove(instance_key);
                    self.forgotten_vars.insert(*instance_key);
                }
                _ => {}
            }
        }
        state::apply(&mut self.state, &event);
        log.push(event);
        if let Some((instance_key, job_key, lease, commit)) = resolution {
            self.settle_job_history(log, instance_key, job_key, &lease, commit);
        }
    }

    fn process_of_instance(&self, instance_key: Key) -> Option<&crate::model::ProcessDefinition> {
        let instance = self.state.instances.get(&instance_key)?;
        self.state.definition_for(instance).map(|p| &p.definition)
    }

    fn element_kind(&self, instance_key: Key, element_id: &str) -> Option<ElementKind> {
        self.process_of_instance(instance_key)?
            .element(element_id)
            .map(|e| e.kind.clone())
    }

    /// Resolves the matching link *catch* element for a link *throw*: the
    /// [`LinkIntermediateCatchEvent`](crate::model::ElementKind::LinkIntermediateCatchEvent)
    /// with the same `link_name` in the same scope (`parent`) as the throw. Deploy
    /// validation guarantees a matching, unique, **same-scope** catch exists (a
    /// cross-scope or empty-named pairing is rejected at deploy), so a well-formed
    /// model always resolves. Only a same-scope catch is returned: activating a
    /// different-scope catch in the throw's runtime scope would corrupt variable
    /// scoping, so that is never done. Returns the catch element id, or `None`
    /// when the process/element is unknown or (defensively) no same-scope catch
    /// exists.
    fn resolve_link_catch(
        &self,
        instance_key: Key,
        throw_id: &str,
        link_name: &str,
    ) -> Option<String> {
        let def = self.process_of_instance(instance_key)?;
        let throw_parent = def.element(throw_id).and_then(|e| e.parent.clone());
        for element in def.elements.values() {
            if let ElementKind::LinkIntermediateCatchEvent { link_name: name } = &element.kind {
                if name == link_name && element.parent == throw_parent {
                    return Some(element.id.clone());
                }
            }
        }
        None
    }

    /// Whether the process instance owning `instance_key` is currently
    /// suspended. A suspended instance makes no progress: its jobs are not
    /// activatable and its timers/other triggers do not fire (Camunda parity).
    fn instance_is_suspended(&self, instance_key: Key) -> bool {
        self.state
            .instances
            .get(&instance_key)
            .is_some_and(|i| i.state == ProcessInstanceState::Suspended)
    }

    fn apply_activation_plan(&mut self, log: &mut Vec<Event>, plan: ActivationPlan) {
        let ActivationPlan {
            job_keys,
            worker,
            timeout,
            now,
            options,
            durable,
        } = plan;
        let mut seen = std::collections::HashSet::new();
        for job_key in job_keys {
            if !seen.insert(job_key) {
                continue;
            }
            let Some(job) = self.state.jobs.get(&job_key) else {
                continue;
            };
            let activatable = job.state == state::JobState::Created
                || (job.state == state::JobState::Activated
                    && !self.job_requires_durable_activation(job_key));
            if !activatable || (!options.with_lease && job.lease_token.is_some()) {
                continue;
            }
            let instance_key = job.instance_key;
            // A suspended instance makes no progress: none of its jobs activate
            // until it is resumed (Camunda parity).
            if self.instance_is_suspended(instance_key) {
                continue;
            }
            // Consuming replicated keys makes issuance deterministic. Replay
            // restores this reservation through Event::max_key.
            let lease_token = options
                .with_lease
                .then(|| crate::lease::issue(self.mint_key()));
            self.emit(
                log,
                Event::JobActivated {
                    job_key,
                    instance_key,
                    durable,
                    worker: worker.clone(),
                    deadline: now.saturating_add(timeout),
                    activated_at: Some(now),
                    fetch_variables: options.fetch_variables.clone(),
                    lease_token,
                },
            );
        }
    }

    fn update_job_properties(
        &mut self,
        log: &mut Vec<Event>,
        job_key: Key,
        retries: Option<i32>,
        timeout: Option<i64>,
        operation_reference: Option<i64>,
        lease_token: Option<&str>,
    ) -> Result<(), EngineError> {
        let job = self
            .state
            .jobs
            .get(&job_key)
            .ok_or(EngineError::JobNotFound { job_key })?;
        if matches!(
            job.state,
            state::JobState::Completed | state::JobState::Canceled
        ) {
            return Err(EngineError::JobNotActive { job_key });
        }
        Self::validate_job_lease(job, lease_token, false)?;
        if retries.is_some_and(|value| value < 1) {
            return Err(EngineError::JobUpdateInvalid {
                job_key,
                reason: "retries must be at least 1".into(),
            });
        }
        if timeout.is_some() && (job.state != state::JobState::Activated || job.deadline.is_none())
        {
            return Err(EngineError::JobUpdateInvalid {
                job_key,
                reason: "timeout requires an active job deadline".into(),
            });
        }
        let instance_key = job.instance_key;
        if let Some(retries) = retries {
            self.emit(
                log,
                Event::JobRetriesUpdated {
                    job_key,
                    instance_key,
                    retries,
                    operation_reference,
                },
            );
        }
        if let Some(timeout) = timeout {
            self.emit(
                log,
                Event::JobTimeoutUpdated {
                    job_key,
                    instance_key,
                    deadline: self.now.saturating_add_signed(timeout),
                    operation_reference,
                },
            );
        }
        Ok(())
    }

    /// Validates a `CompleteJob`'s optional business-id assignment against
    /// the job's process instance (Camunda 8.10). Returns the id to assign, or
    /// `None` when there is nothing to do (no id, or the identical id is
    /// already assigned — an idempotent no-op per the REST contract).
    fn validate_business_id_assignment(
        &self,
        instance_key: Key,
        business_id: Option<String>,
    ) -> Result<Option<String>, EngineError> {
        let Some(business_id) = business_id else {
            return Ok(None);
        };
        let instance = self
            .state
            .instances
            .get(&instance_key)
            .ok_or(EngineError::InstanceNotFound { instance_key })?;
        if instance.parent_process_instance_key.is_some() {
            return Err(EngineError::BusinessIdOnChildInstance { instance_key });
        }
        check_business_id(Some(&business_id))?;
        match instance.business_id.as_deref() {
            Some(existing) if existing == business_id => Ok(None),
            Some(_) => Err(EngineError::BusinessIdAlreadyAssigned { instance_key }),
            None => Ok(Some(business_id)),
        }
    }

    fn validate_job_lease(
        job: &state::Job,
        supplied: Option<&str>,
        required: bool,
    ) -> Result<(), EngineError> {
        let Some(stored) = job.lease_token.as_deref().filter(|token| !token.is_empty()) else {
            return Ok(());
        };
        let supplied = supplied.filter(|token| !token.is_empty());
        if supplied.is_none() && !required {
            return Ok(());
        }
        if supplied != Some(stored) {
            return Err(EngineError::JobLeaseMismatch { job_key: job.key });
        }
        Ok(())
    }

    /// The multi-instance loop characteristics declared on `element_id`, if any.
    fn multi_instance_of(
        &self,
        instance_key: Key,
        element_id: &str,
    ) -> Option<crate::model::MultiInstance> {
        self.process_of_instance(instance_key)?
            .element(element_id)
            .and_then(|e| e.multi_instance.clone())
    }
}

/// An entity owned by a single process instance, identified by `instance_key`.
/// Lets [`drain_owned`] lift every record an instance owns out of a hot-state map
/// in one generic pass during cold-spill snapshotting.
trait OwnedByInstance {
    fn instance_key(&self) -> Key;
}

impl OwnedByInstance for state::Timer {
    fn instance_key(&self) -> Key {
        self.instance_key
    }
}
impl OwnedByInstance for state::MessageSubscription {
    fn instance_key(&self) -> Key {
        self.instance_key
    }
}
impl OwnedByInstance for state::SignalSubscription {
    fn instance_key(&self) -> Key {
        self.instance_key
    }
}
impl OwnedByInstance for state::ConditionalSubscription {
    fn instance_key(&self) -> Key {
        self.instance_key
    }
}
impl OwnedByInstance for state::UserTask {
    fn instance_key(&self) -> Key {
        self.instance_key
    }
}
impl OwnedByInstance for state::Incident {
    fn instance_key(&self) -> Key {
        self.instance_key
    }
}

/// The sorted set of root variable names a conditional event's FEEL `condition`
/// references, used both to record the subscription's dependencies and to decide
/// which variable changes re-evaluate it. Sorted for a deterministic event body.
/// The breadth-first search of Zeebe's `BpmnInclusiveGatewayBehavior`: can a
/// token at `source` reach `join`? It follows outgoing sequence flows, the
/// boundary events attached to an element, and a link throw's same-named catch
/// in the same scope. It skips the join's incoming flows already in `taken`, so
/// a path that ends on a taken flow does not hold the join.
fn path_reaches_join(
    process: &crate::model::ProcessDefinition,
    source: &str,
    join: &str,
    taken: &state::FlowArrivals,
) -> bool {
    let mut visited: HashSet<&str> = HashSet::new();
    let mut to_visit: VecDeque<&str> = VecDeque::from([source]);
    while let Some(node) = to_visit.pop_front() {
        if node == join {
            return true;
        }
        if !visited.insert(node) {
            continue;
        }
        let Some(element) = process.element(node) else {
            continue;
        };
        if let ElementKind::LinkIntermediateThrowEvent { link_name } = &element.kind {
            to_visit.extend(process.elements.values().filter_map(|other| {
                matches!(&other.kind, ElementKind::LinkIntermediateCatchEvent { link_name: name }
                    if name == link_name && other.parent == element.parent)
                .then_some(other.id.as_str())
            }));
            continue;
        }
        for (index, flow) in element.outgoing.iter().enumerate() {
            let taken_into_join = flow.to == join
                && process
                    .incoming_flow(node, index)
                    .is_some_and(|(_, incoming)| taken.count(&incoming) > 0);
            if !taken_into_join {
                to_visit.push_back(flow.to.as_str());
            }
        }
        to_visit.extend(process.elements.values().filter_map(|other| {
            other
                .kind
                .attached_to()
                .is_some_and(|host| host == node)
                .then_some(other.id.as_str())
        }));
    }
    false
}

fn sorted_referenced_vars(condition: &str) -> Vec<String> {
    let mut vars: Vec<String> = crate::feel::referenced_variables(condition)
        .into_iter()
        .collect();
    vars.sort();
    vars
}

/// The postfix Zeebe appends to an ad-hoc sub-process id to form its
/// `AD_HOC_SUB_PROCESS_INNER_INSTANCE` element id
/// (`ZeebeConstants.AD_HOC_SUB_PROCESS_INNER_INSTANCE_ID_POSTFIX`). Kept as the
/// single source of truth so the engine (which mints inner instances) and the
/// read model (which stamps their element type) agree.
pub const ADHOC_INNER_INSTANCE_ID_POSTFIX: &str = "#innerInstance";

/// The element id of the `AD_HOC_SUB_PROCESS_INNER_INSTANCE` that nests the tools
/// of the ad-hoc container `container_element_id`.
pub fn adhoc_inner_instance_id(container_element_id: &str) -> String {
    format!("{container_element_id}{ADHOC_INNER_INSTANCE_ID_POSTFIX}")
}

/// Removes from `map` every entity owned by `instance_key`, returning them. Used
/// to lift an instance's timers/subscriptions/user-tasks/incidents out of hot
/// state when snapshotting it for cold spill.
fn drain_owned<V: OwnedByInstance>(map: &mut HashMap<Key, V>, instance_key: Key) -> Vec<V> {
    let owned: Vec<Key> = map
        .iter()
        .filter(|(_, v)| v.instance_key() == instance_key)
        .map(|(k, _)| *k)
        .collect();
    owned.into_iter().filter_map(|k| map.remove(&k)).collect()
}

/// Errors returned by [`Engine::apply_command`].
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum EngineError {
    /// `CreateInstance` referenced a process id that was never deployed.
    ProcessNotFound { process_id: String },
    /// `EvaluateDecision` referenced a decision (by id or key) that was never
    /// deployed. `reference` describes it (`id 'x'` / `key 123`).
    DecisionNotFound { reference: String },
    /// `DeployProcess` was given a definition whose `start_event` is not among
    /// its elements.
    NoStartEvent { process_id: String },
    /// `CompleteJob` referenced a job key that does not exist.
    JobNotFound { job_key: Key },
    /// `CompleteJob`/`FailJob` referenced a job that is not in a state where it
    /// can be acted on (already completed, or failed with an incident raised).
    JobNotActive { job_key: Key },
    /// A leased lifecycle command omitted its token, or supplied a stale token (409).
    JobLeaseMismatch { job_key: Key },
    /// A requested job property is invalid for the current job (400).
    JobUpdateInvalid { job_key: Key, reason: String },
    /// CREATE cannot replace an agent already associated with the element (409).
    AgentInstanceAlreadyExists {
        element_instance_key: Key,
        agent_instance_key: Key,
    },
    /// Another element's activated job still owns the write claim (409).
    AgentInstanceActiveWriter {
        agent_instance_key: Key,
        element_instance_key: Key,
    },
    /// Invalid history shape, configuration, or request-level history patch (400).
    AgentHistoryInvalid { reason: String },
    /// `CompleteJob`/`FailJob` referenced a job that has never been activated. A
    /// job must be activated at least once before it can be completed or failed.
    JobNotActivated { job_key: Key },
    /// `CompleteJob` tried to assign a business id to a child (call-activity)
    /// process instance; only root instances accept one (409, INVALID_STATE).
    BusinessIdOnChildInstance { instance_key: Key },
    /// A command carried a business id outside the spec's `BusinessId` range
    /// (1..=[`BUSINESS_ID_MAX_CHARS`] characters) — 400. Enforced in the
    /// engine so every surface (REST, Falcon, wasm) shares one check.
    BusinessIdInvalid { chars: usize },
    /// `CompleteJob` tried to assign a business id differing from the one the
    /// instance already carries; assignment is single and irreversible (409).
    BusinessIdAlreadyAssigned { instance_key: Key },
    /// `ResolveIncident` referenced an incident key that does not exist.
    IncidentNotFound { incident_key: Key },
    /// `ResolveIncident` referenced a job-incident whose job still has no
    /// retries; the retries must be updated before it can be resolved.
    IncidentNotResolvable { incident_key: Key, reason: String },
    /// `SetVariables` referenced a scope key that is neither a process instance
    /// nor any active element instance.
    ScopeNotFound { scope_key: Key },
    /// `CancelInstance` referenced a process instance that does not exist or is
    /// no longer active (already completed or terminated).
    InstanceNotFound { instance_key: Key },
    /// A `SuspendInstance` / `ResumeInstance` referenced a process instance that
    /// exists but is in a state from which the requested transition is illegal
    /// (e.g. suspending/resuming an already terminal instance). Only the live
    /// `Active ⇄ Suspended` transitions are valid.
    InstanceTransitionInvalid {
        instance_key: Key,
        from: &'static str,
        to: &'static str,
    },
    /// A `ModifyInstance` activate instruction referenced an element id that is
    /// not part of the instance's process definition.
    ElementNotFound {
        instance_key: Key,
        element_id: String,
    },
    /// A `ModifyInstance` terminate instruction referenced a key that is not an
    /// active element instance of the target process instance.
    ElementInstanceNotFound {
        instance_key: Key,
        element_instance_key: Key,
    },
    /// `AssignUserTask`/`CompleteUserTask` referenced a user-task key that does
    /// not exist.
    UserTaskNotFound { user_task_key: Key },
    /// `AssignUserTask`/`CompleteUserTask` referenced a user task that is not in
    /// a state where it can be acted on (already completed or cancelled).
    UserTaskNotActive { user_task_key: Key },
    /// `AssignUserTask` with `allow_override = false` targeted a user task that
    /// already has an assignee; it must be unassigned first.
    UserTaskAlreadyAssigned { user_task_key: Key },
    /// A task-listener job was completed with variables, which Zeebe forbids for
    /// listener jobs (ADR 0037 §6).
    TaskListenerJobWithVariables { job_key: Key },
    /// A task-listener job denied its transition while also returning
    /// corrections; the two are mutually exclusive (ADR 0037 §6).
    TaskListenerDenyWithCorrections { job_key: Key },
    /// A task-listener job denied a transition whose event type does not support
    /// denial (only `assigning`, `updating`, `completing` do; ADR 0037 §6).
    TaskListenerDenyNotSupported { job_key: Key },
    /// A `creating` task listener tried to correct the assignee of a user task
    /// that already declares an initial assignee (ADR 0037 §6, Zeebe parity).
    TaskListenerAssigneeCorrectionOnCreating { user_task_key: Key },
    /// An ad-hoc sub-process agent's `activateElements[]` instruction referenced
    /// an element id that is not one of the container's tools. Zeebe rejects the
    /// activation with NOT_FOUND (`AdHocSubProcessInstructionActivateProcessor`).
    AdHocUnknownElement {
        instance_key: Key,
        element_id: String,
    },
    /// An ad-hoc sub-process agent asserted `completionConditionFulfilled` while
    /// also requesting new element activations in the same turn. The two are
    /// contradictory; Zeebe rejects with INVALID_ARGUMENT
    /// (`AdHocSubProcessUtils.verifyCompletionConditionFulfilled`).
    AdHocActivateWithCompletion { job_key: Key },
    /// The external "activate ad-hoc activities" command (#614 gap 3) named an
    /// `adHocSubProcessInstanceKey` that does not identify an active ad-hoc
    /// sub-process container. Zeebe rejects with NOT_FOUND
    /// (`AdHocSubProcessInstructionActivateProcessor`).
    AdHocSubProcessNotFound { ad_hoc_instance_key: Key },
    /// The external "activate ad-hoc activities" command (#614 gap 3) was sent
    /// with no `elements` to activate and `cancelRemainingInstances = false`.
    /// That request is a no-op the caller cannot have intended — completing the
    /// container is only expressible via `cancelRemainingInstances` — so it is
    /// rejected as INVALID_ARGUMENT rather than silently finishing a parked
    /// container. (The agent-job completion seam, #614 gap 4, still ends a turn
    /// by activating nothing; only this external command forbids it.)
    AdHocNoActivationTargets { ad_hoc_instance_key: Key },
    /// A `MigrateInstance` named a `target_process_definition_key` that is not
    /// deployed (or is not the latest retained version of its process id). Maps
    /// to HTTP 404. Zeebe parity: "expected to migrate ... but the target process
    /// definition could not be found".
    TargetProcessDefinitionNotFound { process_definition_key: Key },
    /// A `MigrateInstance` mapping list contained the same `source_element_id`
    /// twice. Maps to HTTP 400. Zeebe parity: "the mapping instructions ... target
    /// the same source element more than once".
    DuplicateMappingSourceElement {
        instance_key: Key,
        element_id: String,
    },
    /// A `MigrateInstance` mapping referenced a `source_element_id` absent from
    /// the instance's (current) process definition. Maps to HTTP 400.
    MappingSourceElementNotFound {
        instance_key: Key,
        element_id: String,
    },
    /// A `MigrateInstance` mapping referenced a `target_element_id` absent from
    /// the target process definition. Maps to HTTP 400.
    MappingTargetElementNotFound {
        target_process_definition_key: Key,
        element_id: String,
    },
    /// A `MigrateInstance` left an active element instance without a mapping
    /// instruction. Maps to HTTP 409. Zeebe parity: "no mapping instruction
    /// defined for active element with id".
    UnmappedActiveElement {
        instance_key: Key,
        element_id: String,
    },
    /// A `MigrateInstance` mapped a source element to a target element of a
    /// different BPMN type. Maps to HTTP 409. Zeebe parity: "active element ...
    /// is mapped to an element with id ... and different type".
    MappedElementTypeChanged {
        instance_key: Key,
        source_element_id: String,
        target_element_id: String,
    },
    /// A `MigrateInstance` mapped an already-open join (a token has arrived on
    /// some but not all of its incoming flows) onto a target gateway with a
    /// different number of incoming sequence flows, where the join's partial
    /// count would be re-read against the target's arity: every open parallel
    /// join, and an inclusive join holding unidentified arrivals (#1241). A
    /// mismatch would early-fire or deadlock the migrated join. Maps to HTTP 409.
    MigratedParallelJoinArityChanged {
        instance_key: Key,
        source_element_id: String,
        target_element_id: String,
        source_incoming_count: usize,
        target_incoming_count: usize,
    },
    /// A `MigrateInstance` mapped an open parallel- or inclusive-gateway join that has counted
    /// a token on an incoming flow (the `flow_ordinal`-th flow from
    /// `flow_source_element_id` to the join) that is not an incoming flow of the
    /// target gateway. The migrated join would count a flow that no longer
    /// exists and fire early (#1233). Maps to HTTP 409.
    MigratedJoinFlowMissing {
        instance_key: Key,
        source_element_id: String,
        target_element_id: String,
        flow_source_element_id: String,
        flow_ordinal: usize,
    },
    /// A `MigrateInstance` targeted an instance that contains an active element
    /// class this phase does not yet support migrating (boundary events, event
    /// subprocesses, multi-instance bodies, call activities, event-based-gateway
    /// catch events). Maps to HTTP 409. Zeebe itself rejects several of these as
    /// "not supported yet"; reproducing the rejection is parity.
    UnsupportedMigration {
        instance_key: Key,
        element_id: String,
        reason: String,
    },
    /// An AgentInstance command referenced an `agent_instance_key` that does not
    /// identify a known agent instance.
    AgentInstanceNotFound { agent_instance_key: Key },
    /// A CREATE/UPDATE command referenced an `element_instance_key` that is not
    /// an *active* element instance (unknown, or already completed/terminated).
    AgentInstanceElementInstanceInactive { element_instance_key: Key },
    /// A CREATE command referenced an active element instance whose element is
    /// not agent-eligible (not a service task / ad-hoc sub-process).
    AgentInstanceElementNotEligible { element_instance_key: Key },
    /// A CREATE command referenced an eligible element *type* (service task /
    /// ad-hoc sub-process) that carries no `agentDefinition` (`agentDefinitionKey`).
    AgentInstanceMissingAgentDefinition { element_instance_key: Key },
    /// A worker-issued CREATE (or history-bearing UPDATE) for an `external`,
    /// job-backed agent element attached a history batch but supplied **no**
    /// `jobKey` (`job_key == 0`). Maps to Camunda's `JOB_REQUIRED_FOR_HISTORY`
    /// (`INVALID_ARGUMENT`, HTTP **400**): a history batch must be attributed to
    /// the active job that produced it.
    AgentInstanceJobRequiredForHistory { element_instance_key: Key },
    /// A CREATE/UPDATE for an `external` agent referenced a `job_key` that is not
    /// a currently-**ACTIVATED** job (unknown, expired, completed, …). Maps to
    /// Camunda's `JOB_NOT_ACTIVE` (`NOT_FOUND`, HTTP **404**).
    AgentInstanceJobNotActive {
        element_instance_key: Key,
        job_key: Key,
    },
    /// A CREATE/UPDATE for an `external` agent referenced an ACTIVATED job whose
    /// opaque lease token does not match the supplied `job_lease` — the job was
    /// re-activated since the lease was issued. Maps to Camunda's
    /// `JOB_LEASE_MISMATCH` (`NOT_FOUND`, HTTP **404**).
    AgentInstanceJobLeaseMismatch {
        element_instance_key: Key,
        job_key: Key,
    },
    /// A CREATE/UPDATE for an `external` agent referenced an ACTIVATED job that
    /// belongs to a *different* element instance than the one requested. Maps to
    /// Camunda's `JOB_ELEMENT_MISMATCH` (`INVALID_ARGUMENT`, HTTP **400**).
    AgentInstanceJobElementMismatch {
        job_key: Key,
        job_element_instance_key: Key,
        element_instance_key: Key,
    },
    /// An UPDATE command's asserted `element_id` / `process_instance_key` do not
    /// match the stored agent instance (a stale or misrouted update).
    AgentInstanceOwnershipMismatch { agent_instance_key: Key },
    /// An UPDATE command referenced an `element_instance_key` already owned by a
    /// *different* agent instance (a conflicting instance).
    AgentInstanceConflict {
        agent_instance_key: Key,
        element_instance_key: Key,
        conflicting_agent_instance_key: Key,
    },
    /// An UPDATE command tried to set a status that is not settable via UPDATE
    /// (only the *active* states are; `COMPLETED` is reached only via COMPLETE).
    AgentInstanceStatusNotSettable {
        agent_instance_key: Key,
        status: crate::agent::AgentInstanceStatus,
    },
    /// A COMPLETE command targeted an agent instance that is already terminal
    /// (`COMPLETED`); completion is not idempotent-repeatable.
    AgentInstanceAlreadyCompleted { agent_instance_key: Key },
    /// A CREATE/UPDATE batch would push the instance's cumulative metrics over a
    /// configured (`!= -1`) limit; the batch is rejected and nothing is applied.
    AgentInstanceLimitExceeded {
        agent_instance_key: Key,
        limit: crate::agent::AgentLimitKind,
    },
}

/// Element classes this migration phase does not remap yet, mirroring Zeebe's
/// "not supported yet" migration rejections. Returns `Some(reason)` when a
/// migration touching an active element (or armed boundary runtime) of this kind
/// must be rejected with [`EngineError::UnsupportedMigration`].
fn unsupported_migration_reason(kind: &crate::model::ElementKind) -> Option<&'static str> {
    use crate::model::ElementKind;
    match kind {
        ElementKind::ErrorBoundaryEvent { .. }
        | ElementKind::TimerBoundaryEvent { .. }
        | ElementKind::MessageBoundaryEvent { .. }
        | ElementKind::SignalBoundaryEvent { .. }
        | ElementKind::ConditionalBoundaryEvent { .. }
        | ElementKind::EscalationBoundaryEvent { .. } => {
            Some("boundary events are not migratable yet")
        }
        ElementKind::CallActivity { .. } => Some("call activities are not migratable yet"),
        ElementKind::SubProcess { .. } => Some("embedded sub-processes are not migratable yet"),
        ElementKind::EventBasedGateway => Some("event-based gateways are not migratable yet"),
        _ => None,
    }
}

impl std::fmt::Display for EngineError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            EngineError::JobLeaseMismatch { job_key } => write!(f, "job {job_key} requires the current activation lease token"),
            EngineError::JobUpdateInvalid { job_key, reason } => write!(f, "invalid update for job {job_key}: {reason}"),
            EngineError::AgentInstanceAlreadyExists { element_instance_key, agent_instance_key } => write!(f, "element instance {element_instance_key} is already associated with agent instance {agent_instance_key}"),
            EngineError::AgentInstanceActiveWriter { agent_instance_key, element_instance_key } => write!(f, "agent instance {agent_instance_key} still has an active writer on element instance {element_instance_key}"),
            EngineError::AgentHistoryInvalid { reason } => write!(f, "invalid agent history: {reason}"),
            EngineError::ProcessNotFound { process_id } => {
                write!(f, "no deployed process with id {process_id}")
            }
            EngineError::DecisionNotFound { reference } => {
                write!(f, "no deployed decision with {reference}")
            }
            EngineError::NoStartEvent { process_id } => {
                write!(
                    f,
                    "process {process_id} has no start event among its elements"
                )
            }
            EngineError::JobNotFound { job_key } => write!(f, "no job with key {job_key}"),
            EngineError::JobNotActive { job_key } => {
                write!(f, "job {job_key} is not in a state that can be acted on")
            }
            EngineError::JobNotActivated { job_key } => {
                write!(
                    f,
                    "job {job_key} must be activated before it can be completed or failed"
                )
            }
            EngineError::BusinessIdOnChildInstance { instance_key } => write!(
                f,
                "cannot assign a business id to process instance {instance_key}: it is a child process instance; a business id can only be assigned to root process instances"
            ),
            EngineError::BusinessIdInvalid { chars } => write!(
                f,
                "invalid business id: must be 1 to {BUSINESS_ID_MAX_CHARS} characters, got {chars}"
            ),
            EngineError::BusinessIdAlreadyAssigned { instance_key } => write!(
                f,
                "cannot assign a business id to process instance {instance_key}: it already has a different business id assigned"
            ),
            EngineError::IncidentNotFound { incident_key } => {
                write!(f, "no incident with key {incident_key}")
            }
            EngineError::IncidentNotResolvable {
                incident_key,
                reason,
            } => {
                write!(f, "incident {incident_key} cannot be resolved: {reason}")
            }
            EngineError::ScopeNotFound { scope_key } => {
                write!(f, "no variable scope with key {scope_key}")
            }
            EngineError::InstanceNotFound { instance_key } => {
                write!(f, "no active process instance with key {instance_key}")
            }
            EngineError::InstanceTransitionInvalid {
                instance_key,
                from,
                to,
            } => {
                write!(
                    f,
                    "process instance {instance_key} cannot transition from {from} to {to}"
                )
            }
            EngineError::ElementNotFound {
                instance_key,
                element_id,
            } => {
                write!(
                    f,
                    "instance {instance_key} has no element with id {element_id}"
                )
            }
            EngineError::ElementInstanceNotFound {
                instance_key,
                element_instance_key,
            } => {
                write!(
                    f,
                    "instance {instance_key} has no active element instance {element_instance_key}"
                )
            }
            EngineError::UserTaskNotFound { user_task_key } => {
                write!(f, "no user task with key {user_task_key}")
            }
            EngineError::UserTaskNotActive { user_task_key } => {
                write!(
                    f,
                    "user task {user_task_key} is not in a state that can be acted on"
                )
            }
            EngineError::UserTaskAlreadyAssigned { user_task_key } => {
                write!(
                    f,
                    "user task {user_task_key} is already assigned; unassign it before assigning again"
                )
            }
            EngineError::TaskListenerJobWithVariables { job_key } => {
                write!(
                    f,
                    "task-listener job {job_key} cannot be completed with variables"
                )
            }
            EngineError::TaskListenerDenyWithCorrections { job_key } => {
                write!(
                    f,
                    "task-listener job {job_key} cannot both deny the transition and return corrections"
                )
            }
            EngineError::TaskListenerDenyNotSupported { job_key } => {
                write!(
                    f,
                    "task-listener job {job_key} denied a transition whose event type does not support denial"
                )
            }
            EngineError::TaskListenerAssigneeCorrectionOnCreating { user_task_key } => {
                write!(
                    f,
                    "a creating task listener cannot correct the assignee of user task {user_task_key}: it already has an initial assignee"
                )
            }
            EngineError::AdHocUnknownElement {
                instance_key,
                element_id,
            } => {
                write!(
                    f,
                    "ad-hoc sub-process in instance {instance_key} has no activatable element with id {element_id}"
                )
            }
            EngineError::AdHocActivateWithCompletion { job_key } => {
                write!(
                    f,
                    "ad-hoc agent job {job_key} cannot both assert the completion condition is fulfilled and activate elements"
                )
            }
            EngineError::AdHocSubProcessNotFound {
                ad_hoc_instance_key,
            } => {
                write!(
                    f,
                    "no active ad-hoc sub-process container with instance key {ad_hoc_instance_key}"
                )
            }
            EngineError::AdHocNoActivationTargets {
                ad_hoc_instance_key,
            } => {
                write!(
                    f,
                    "ad-hoc sub-process activation for container {ad_hoc_instance_key} named no elements and did not cancel remaining instances"
                )
            }
            EngineError::TargetProcessDefinitionNotFound {
                process_definition_key,
            } => {
                write!(
                    f,
                    "no deployed process definition with key {process_definition_key} to migrate to"
                )
            }
            EngineError::DuplicateMappingSourceElement {
                instance_key,
                element_id,
            } => {
                write!(
                    f,
                    "migration of instance {instance_key} maps source element {element_id} more than once"
                )
            }
            EngineError::MappingSourceElementNotFound {
                instance_key,
                element_id,
            } => {
                write!(
                    f,
                    "migration of instance {instance_key} maps source element {element_id}, which its process definition does not contain"
                )
            }
            EngineError::MappingTargetElementNotFound {
                target_process_definition_key,
                element_id,
            } => {
                write!(
                    f,
                    "migration target process definition {target_process_definition_key} has no element with id {element_id}"
                )
            }
            EngineError::UnmappedActiveElement {
                instance_key,
                element_id,
            } => {
                write!(
                    f,
                    "migration of instance {instance_key} leaves active element {element_id} unmapped"
                )
            }
            EngineError::MappedElementTypeChanged {
                instance_key,
                source_element_id,
                target_element_id,
            } => {
                write!(
                    f,
                    "migration of instance {instance_key} maps element {source_element_id} to {target_element_id} of a different type"
                )
            }
            EngineError::MigratedParallelJoinArityChanged {
                instance_key,
                source_element_id,
                target_element_id,
                source_incoming_count,
                target_incoming_count,
            } => {
                write!(
                    f,
                    "migration of instance {instance_key} maps open join {source_element_id} \
                     ({source_incoming_count} incoming flows) to {target_element_id} \
                     ({target_incoming_count} incoming flows); an in-flight join can only map to a \
                     gateway with the same number of incoming sequence flows"
                )
            }
            EngineError::MigratedJoinFlowMissing {
                instance_key,
                source_element_id,
                target_element_id,
                flow_source_element_id,
                flow_ordinal,
            } => {
                write!(
                    f,
                    "migration of instance {instance_key} maps open join {source_element_id} \
                     to {target_element_id}, but the join holds a token on flow #{flow_ordinal} from \
                     {flow_source_element_id}, which is not an incoming flow of {target_element_id}"
                )
            }
            EngineError::UnsupportedMigration {
                instance_key,
                element_id,
                reason,
            } => {
                write!(
                    f,
                    "migration of instance {instance_key} is not supported yet: active element {element_id} ({reason})"
                )
            }
            EngineError::AgentInstanceNotFound { agent_instance_key } => {
                write!(f, "no agent instance with key {agent_instance_key}")
            }
            EngineError::AgentInstanceElementInstanceInactive {
                element_instance_key,
            } => {
                write!(
                    f,
                    "element instance {element_instance_key} is not active for an agent-instance command"
                )
            }
            EngineError::AgentInstanceElementNotEligible {
                element_instance_key,
            } => {
                write!(
                    f,
                    "element instance {element_instance_key} is not an agent-eligible element (service task / ad-hoc sub-process)"
                )
            }
            EngineError::AgentInstanceMissingAgentDefinition {
                element_instance_key,
            } => {
                write!(
                    f,
                    "element instance {element_instance_key} has no agentDefinition (agentDefinitionKey)"
                )
            }
            EngineError::AgentInstanceJobRequiredForHistory {
                element_instance_key,
            } => {
                write!(
                    f,
                    "agent-instance command for element instance {element_instance_key} attached a history batch but supplied no jobKey; a history batch must be attributed to the active job that produced it"
                )
            }
            EngineError::AgentInstanceJobNotActive {
                element_instance_key,
                job_key,
            } => {
                write!(
                    f,
                    "agent-instance command for element instance {element_instance_key} references job {job_key}, but that job is not active"
                )
            }
            EngineError::AgentInstanceJobLeaseMismatch {
                element_instance_key,
                job_key,
            } => {
                write!(
                    f,
                    "agent-instance command for element instance {element_instance_key} references job {job_key}, but the job did not hold the supplied lease (it may have been re-activated)"
                )
            }
            EngineError::AgentInstanceJobElementMismatch {
                job_key,
                job_element_instance_key,
                element_instance_key,
            } => {
                write!(
                    f,
                    "agent-instance command references job {job_key}, but that job belongs to element instance {job_element_instance_key} instead of the requested element instance {element_instance_key}"
                )
            }
            EngineError::AgentInstanceOwnershipMismatch { agent_instance_key } => {
                write!(
                    f,
                    "agent instance {agent_instance_key} update does not match its element/process instance"
                )
            }
            EngineError::AgentInstanceConflict {
                agent_instance_key,
                element_instance_key,
                conflicting_agent_instance_key,
            } => {
                write!(
                    f,
                    "agent instance {agent_instance_key} update references element instance {element_instance_key} already owned by agent instance {conflicting_agent_instance_key}"
                )
            }
            EngineError::AgentInstanceStatusNotSettable {
                agent_instance_key,
                status,
            } => {
                write!(
                    f,
                    "status {} is not settable on agent instance {agent_instance_key} via UPDATE",
                    status.as_str()
                )
            }
            EngineError::AgentInstanceAlreadyCompleted { agent_instance_key } => {
                write!(
                    f,
                    "agent instance {agent_instance_key} is already completed"
                )
            }
            EngineError::AgentInstanceLimitExceeded {
                agent_instance_key,
                limit,
            } => {
                write!(
                    f,
                    "agent instance {agent_instance_key} batch exceeds its {} limit",
                    limit.as_str()
                )
            }
        }
    }
}

impl std::error::Error for EngineError {}

/// A job handed to a worker by [`Engine::activate_jobs`]: everything the worker
/// needs to do the work and complete it by key.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct ActivatedJob {
    pub key: Key,
    pub job_type: String,
    pub instance_key: Key,
    pub element_instance_key: Key,
    pub element_id: String,
    /// The BPMN process id of the job's process definition (Zeebe
    /// `ActivatedJob.bpmnProcessId`).
    pub bpmn_process_id: String,
    /// The key of the job's process definition (Zeebe
    /// `ActivatedJob.processDefinitionKey`).
    pub process_definition_key: Key,
    /// The version of the job's process definition (Zeebe
    /// `ActivatedJob.processDefinitionVersion`).
    pub process_definition_version: i32,
    /// The worker the job was locked to.
    pub worker: String,
    /// Logical instant at which the activation lock expires.
    pub deadline: u64,
    /// Opaque token for an explicitly leased activation, distinct from `deadline`.
    /// Every job kind supports leasing. `None` when the worker did not opt in.
    pub lease_token: Option<String>,
    /// Remaining retries for this job.
    pub retries: i32,
    /// Activation priority (higher is activated first; Zeebe
    /// `ActivatedJob.priority`).
    pub priority: i32,
    /// Static custom headers declared on the task via `zeebe:taskHeaders`,
    /// surfaced verbatim (Zeebe `ActivatedJob.customHeaders`). Empty for jobs
    /// that are not ordinary BPMN-element jobs or whose task declares none. A
    /// `BTreeMap` for deterministic serialization order.
    pub custom_headers: std::collections::BTreeMap<String, String>,
    /// User-defined tags on the owning process instance (Zeebe
    /// `ActivatedJob.tags`).
    pub tags: Vec<String>,
    /// The owning process instance's business id, if any (Zeebe
    /// `ActivatedJob.businessId`).
    pub business_id: Option<String>,
    /// A snapshot of the instance's variables at activation time. Shared via
    /// `Arc` with the engine's instance state, so activation does not deep-clone
    /// the (up to 50 KB) value tree on the single command thread; the response
    /// mapper encodes it to JSON off-thread by borrowing.
    pub variables: Arc<HashMap<String, Value>>,
    /// Whether this is an ordinary BPMN-element job or an execution-listener job
    /// (ADR 0037), surfaced so the worker/transport can report `jobKind` and
    /// `listenerEventType` (Camunda parity).
    pub kind: state::JobKind,
}

/// Whether a job can be activated at the logical instant `now`: it is created
/// (never activated, or its lock was released) or its current lock has expired.
/// Failed (incident-parked) and completed jobs are never activatable.
fn job_activatable(job: &state::Job, now: u64) -> bool {
    match job.state {
        state::JobState::Completed
        | state::JobState::Failed
        | state::JobState::Errored
        | state::JobState::Canceled => false,
        state::JobState::Created => true,
        state::JobState::Activated => job.deadline.is_some_and(|d| d <= now),
    }
}

#[cfg(test)]
mod tests;

/// The spec's `BusinessId` maximum length (`spec/identifiers.yaml`,
/// `maxLength: 256`), counted in characters as JSON Schema does.
pub const BUSINESS_ID_MAX_CHARS: usize = 256;

/// Longest job error message (in characters) the engine records, matching
/// Zeebe's `EngineConfiguration.DEFAULT_MAX_ERROR_MESSAGE_SIZE` (#1327).
pub const MAX_ERROR_MESSAGE_LEN: usize = 10_000;

/// Limits a worker-reported job error message like Zeebe's
/// `StringUtil.limitString(msg, DEFAULT_MAX_ERROR_MESSAGE_SIZE)`: a message
/// longer than [`MAX_ERROR_MESSAGE_LEN`] **UTF-16 code units** (Java
/// `String.length()`) keeps its first that-many units followed by `"..."`. Unlike
/// Java's `substring`, it never splits a surrogate pair: an astral character
/// that would straddle the bound is dropped whole.
pub fn limit_error_message(message: String) -> String {
    let mut units = 0usize;
    for (idx, ch) in message.char_indices() {
        units += ch.len_utf16();
        if units > MAX_ERROR_MESSAGE_LEN {
            let mut limited = message;
            limited.truncate(idx);
            limited.push_str("...");
            return limited;
        }
    }
    message
}

/// The single business-id range check (1..=[`BUSINESS_ID_MAX_CHARS`]
/// characters) applied to every command that carries one. `None` is valid.
/// Public so a host fanning one command out to several partitions can reject
/// up front (atomically) with the engine's own rule.
pub fn check_business_id(business_id: Option<&str>) -> Result<(), EngineError> {
    let Some(id) = business_id else {
        return Ok(());
    };
    let chars = id.chars().count();
    if (1..=BUSINESS_ID_MAX_CHARS).contains(&chars) {
        Ok(())
    } else {
        Err(EngineError::BusinessIdInvalid { chars })
    }
}
