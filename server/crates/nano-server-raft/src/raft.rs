//! Per-partition Raft (stage 3): every partition is a Raft group with a movable
//! leader; a command commits when a quorum of the replica set has the log entry,
//! then applies to the engine. This module wires [openraft] over our existing
//! durable [`Journal`] (the state machine) and — in a later milestone — the
//! Falcon protocol (the network).
//!
//! # Milestone status (RF=1, single voter)
//!
//! This first slice proves the integration end to end for a single-node Raft
//! group: a [`ReplicatedCommand`] flows through `client_write` → the replicated
//! log → [`RaftStateMachine::apply`], which applies it to the partition's
//! [`Journal`] and awaits its durable [`Commit`]. With one voter the quorum is
//! itself, so commit is immediate and the network layer is never exercised.
//!
//! Deliberately **additive**: this does not yet replace the server's
//! [`DeepthiHandle`](crate::deepthi::DeepthiHandle) write path.
//!
//! # Milestone B: crash-durable log (done)
//!
//! [`bootstrap_single_durable`](RaftPartition::bootstrap_single_durable) backs the
//! Raft log with [`RaftLogStore`](crate::raft_logstore::RaftLogStore), an
//! `fsync`-on-append disk log. By the Raft model the **log is the source of
//! truth**: a client-acked command is durable once it is in that log, and a
//! restart replays the durable log back through the (volatile) state machine to
//! reconstruct engine state — so the engine [`Journal`] itself can stay
//! in-memory. [`bootstrap_single`](RaftPartition::bootstrap_single) keeps the
//! original in-memory [`MemLogStore`] for tests that don't need durability.
//!
//! # Milestone C: multi-voter network (done)
//!
//! [`bootstrap_member`](RaftPartition::bootstrap_member) +
//! [`initialize`](RaftPartition::initialize) form an RF>1 group whose replicas
//! exchange AppendEntries/Vote/InstallSnapshot through a
//! [`RaftTransport`](crate::raft_net::RaftTransport) (see [`crate::raft_net`]).
//! The transport is pluggable: the in-process
//! [`LocalCluster`](crate::raft_net::LocalCluster) proves replication + commit
//! across a real 3-voter group, and a falcon-backed carrier mounts the
//! same network onto the cluster WebSocket once the server hosts the Raft groups.
//!
//! # Remaining
//!
//! Leader routing: host the Raft groups in the server, carry the
//! [`RaftTransport`](crate::raft_net::RaftTransport) over the Falcon protocol, and
//! route client writes to the partition leader — replacing the additive
//! [`DeepthiHandle`](crate::deepthi::DeepthiHandle) write path.

// This Raft subsystem (raft / raft_logstore / raft_net) is built up across
// stage-3 milestones and is deliberately *additive*: it is fully exercised by
// its own unit tests but not yet mounted on the server's production write path
// (that lands with leader routing). Until then, several public items are unused
// in a plain `cargo build`, so dead-code is allowed at the module level.
#![allow(dead_code)]

use std::collections::{BTreeMap, HashMap};
use std::fmt::Debug;
use std::io::Write as _;
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::pin::Pin;
use std::sync::atomic::{AtomicI64, AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use nanobpmn_engine_core::{Command, Event};
use openraft::error::{ClientWriteError, RaftError};
use openraft::storage::{
    LogFlushed, LogState, RaftLogReader, RaftLogStorage, RaftStateMachine, Snapshot,
};
use openraft::{
    BasicNode, Config, Entry, EntryPayload, ErrorSubject, ErrorVerb, LogId, OptionalSend,
    RaftSnapshotBuilder, SnapshotMeta, StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use crate::deepthi::{DeepthiHandle, LifecycleExclusive, LifecycleShared, SnapshotPublishGuard};
#[cfg(test)]
use crate::journal::Journal;
use crate::raft_net::{NullTransport, PartitionNetwork, RaftTransport, SnapshotSendProgress};

/// Raft node id. We key the cluster by the topology's `node_id` (a `u32`),
/// widened to openraft's expected `u64`.
pub type NodeId = u64;

openraft::declare_raft_types!(
    /// The Raft type configuration for a nanobpmn partition group.
    pub RaftConfig:
        D = ReplicatedBatch,
        R = ReplicatedResponse,
        SnapshotData = SnapshotFile,
);

/// The unit replicated through the Raft log: an engine [`Command`] plus the
/// wall-clock `now` the leader stamped it with. Carrying `now` keeps `apply`
/// deterministic across replicas (the engine's time-dependent logic replays
/// identically), so re-applying the log on any replica yields identical state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ReplicatedCommand {
    pub command: Command,
    pub now: u64,
}

/// Process-global net-live count of [`ReplicatedBatch`] instances. Every batch
/// carries a [`BatchLiveGuard`] that increments this on construction (leader
/// propose), deserialization (receive path — via serde's `default` for the
/// skipped field) and clone, and decrements it on drop. Published to
/// `nanobpm_raft_live_batches`.
///
/// This is the **replication-window occupancy** gauge. At idle it plateaus at
/// roughly `retained_log_streams × KEEP_LOGS` (openraft retains the recent,
/// non-purged tail of each replicated log in memory to catch up lagging
/// replicas without a fresh snapshot install). That retention lives *outside*
/// [`RaftLogStore`](crate::raft_logstore::RaftLogStore) (which demotes its own
/// copies to disk), so it is invisible to `nanobpm_raft_log_ram_bytes`. This
/// gauge (with [`LIVE_BATCH_BYTES`]) makes the retention legible: a bounded,
/// throughput-proportional plateau — **accounted replication state, not a
/// leak**.
pub static LIVE_BATCHES: AtomicI64 = AtomicI64::new(0);

/// Process-global net-live sum of the exact serialized byte size of every live
/// [`ReplicatedBatch`], published to `nanobpm_raft_live_batch_bytes`. Each
/// guard carries its batch's size (computed once, zero-alloc, via
/// [`serialized_len`]) and adds/subtracts it here on construct/clone/drop, so
/// this is the true resident byte footprint of the live batch population
/// regardless of *which* structure (ours or openraft's) retains them. This is
/// the byte magnitude of the replication-window retention counted by
/// [`LIVE_BATCHES`]; under fat coalesced payloads it dominates resident heap yet
/// stays bounded by the keep window.
pub static LIVE_BATCH_BYTES: AtomicI64 = AtomicI64::new(0);

/// The current net-live [`ReplicatedBatch`] count (see [`LIVE_BATCHES`]).
pub fn live_batches() -> i64 {
    LIVE_BATCHES.load(Ordering::Relaxed)
}

/// The current net-live [`ReplicatedBatch`] byte footprint (see
/// [`LIVE_BATCH_BYTES`]).
pub fn live_batch_bytes() -> i64 {
    LIVE_BATCH_BYTES.load(Ordering::Relaxed)
}

/// The exact JSON-serialized byte length of `value`, computed without
/// allocating an output buffer (a counting [`std::io::Write`] sink). Used to
/// size a batch's payload for [`LIVE_BATCH_BYTES`] once, at construction /
/// deserialization.
fn serialized_len<T: Serialize>(value: &T) -> u64 {
    #[derive(Default)]
    struct Counter(u64);
    impl std::io::Write for Counter {
        fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
            self.0 += buf.len() as u64;
            Ok(buf.len())
        }
        fn flush(&mut self) -> std::io::Result<()> {
            Ok(())
        }
    }
    let mut c = Counter::default();
    // Serialization of a well-formed value never fails against an infallible
    // writer; fall back to 0 rather than panic on the metrics path.
    match serde_json::to_writer(&mut c, value) {
        Ok(()) => c.0,
        Err(_) => 0,
    }
}

/// RAII guard that keeps [`LIVE_BATCHES`]/[`LIVE_BATCH_BYTES`] in step with the
/// count and byte footprint of live [`ReplicatedBatch`] values. `bytes` is this
/// batch's serialized size, so `Clone` re-adds it and `Drop` subtracts exactly
/// the same amount.
#[derive(Debug)]
pub struct BatchLiveGuard {
    bytes: u64,
}

impl BatchLiveGuard {
    fn with_bytes(bytes: u64) -> Self {
        LIVE_BATCHES.fetch_add(1, Ordering::Relaxed);
        LIVE_BATCH_BYTES.fetch_add(bytes as i64, Ordering::Relaxed);
        BatchLiveGuard { bytes }
    }
}

impl Default for BatchLiveGuard {
    // Constructs a zero-byte guard; real construction/deserialization paths size
    // the guard explicitly (`ReplicatedBatch::new`/its `Deserialize` impl).
    fn default() -> Self {
        Self::with_bytes(0)
    }
}

impl Clone for BatchLiveGuard {
    // A cloned batch is a distinct live value with the same footprint, so its
    // guard must re-register both count and bytes (the derived clone would copy
    // the fields without counting, driving the gauges negative on drop).
    fn clone(&self) -> Self {
        Self::with_bytes(self.bytes)
    }
}

impl Drop for BatchLiveGuard {
    fn drop(&mut self) {
        LIVE_BATCHES.fetch_sub(1, Ordering::Relaxed);
        LIVE_BATCH_BYTES.fetch_sub(self.bytes as i64, Ordering::Relaxed);
    }
}

/// A **batch** of commands replicated as a single Raft log entry. Coalescing
/// many concurrently-proposed commands into one entry amortizes openraft's
/// per-entry overhead (one append + one replication round-trip + one apply
/// round-trip + one engine-actor hop for the whole batch) across all of them —
/// the dominant write-path cost under load. A batch of one (the default for a
/// lone proposer, e.g. tests or deploy) is byte-for-byte the prior behavior.
#[derive(Clone, Debug, Serialize)]
pub struct ReplicatedBatch {
    pub items: Vec<ReplicatedCommand>,
    /// Live-count/-bytes guard (see [`LIVE_BATCHES`]/[`LIVE_BATCH_BYTES`]).
    /// Skipped on the wire (reconstructed on deserialize), so the replicated
    /// bytes are unchanged.
    #[serde(skip)]
    _live: BatchLiveGuard,
}

// Manual `Deserialize` (rather than derive + `#[serde(skip)]`/`default`) so the
// receive / log-read-back path — where openraft's retained copies are born —
// sizes the guard from the just-deserialized `items`, keeping
// `LIVE_BATCH_BYTES` exact regardless of which structure retains the batch.
impl<'de> serde::Deserialize<'de> for ReplicatedBatch {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        #[derive(serde::Deserialize)]
        struct Wire {
            items: Vec<ReplicatedCommand>,
        }
        let Wire { items } = Wire::deserialize(deserializer)?;
        Ok(ReplicatedBatch::new(items))
    }
}

impl ReplicatedBatch {
    /// Build a batch from its commands, sizing the live-bytes guard once.
    pub fn new(items: Vec<ReplicatedCommand>) -> Self {
        let bytes = serialized_len(&items);
        Self {
            items,
            _live: BatchLiveGuard::with_bytes(bytes),
        }
    }

    /// A single-command batch (the convenience path for `propose`).
    pub fn single(command: Command, now: u64) -> Self {
        Self::new(vec![ReplicatedCommand { command, now }])
    }
}

/// The per-command outcome within a committed batch: the events the command
/// produced (so the caller can drive read-model export, routing and completion
/// exactly as the direct engine path does), or — when the engine *rejected* the
/// command (e.g. a complete on a non-existent job) — the mapped `(http_status,
/// message)`. A rejected command is still consumed on every replica (as a no-op)
/// so replicas stay in lockstep; only the leader surfaces the rejection.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicatedItem {
    pub events: Vec<Event>,
    #[serde(default)]
    pub error: Option<(u16, String)>,
}

/// Why a [`RaftPartition::propose_result`] failed before the command could be
/// replicated. The propose path runs an openraft `client_write`, whose typed
/// error distinguishes a *leadership* failure (this node is not — or is no
/// longer — the leader, so the write must be retried against the new leader)
/// from a genuine internal fault. The batcher used to stringify that error,
/// collapsing both into an opaque message the HTTP layer could only map to a
/// 500; preserving the distinction lets a transient election surface as the
/// retryable 503 the mutation funnels document. #1306.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum ProposeError {
    /// The propose was rejected because this node is not the partition's
    /// current leader (openraft `ForwardToLeader`), or leadership was lost
    /// while the batched `client_write` was in flight. Retryable: the caller
    /// should re-resolve the leader and retry.
    Leader,
    /// Any other propose failure (storage fault, batcher teardown, arity
    /// mismatch, …). Not a leadership race; surfaced as an internal error.
    Other(String),
}

impl std::fmt::Display for ProposeError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ProposeError::Leader => write!(f, "partition leader unavailable; retry"),
            ProposeError::Other(msg) => write!(f, "raft propose failed: {msg}"),
        }
    }
}

impl std::error::Error for ProposeError {}

impl ProposeError {
    /// Classifies an openraft `client_write` error, preserving the leadership
    /// distinction the batcher would otherwise stringify away.
    fn from_client_write(e: RaftError<NodeId, ClientWriteError<NodeId, BasicNode>>) -> Self {
        if e.forward_to_leader().is_some() {
            ProposeError::Leader
        } else {
            ProposeError::Other(e.to_string())
        }
    }
}

/// The result handed back to the `client_write` caller on the leader: one
/// [`ReplicatedItem`] per command in the proposed batch, in submission order.
/// Only meaningful on the applying leader; followers discard it.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct ReplicatedResponse {
    pub items: Vec<ReplicatedItem>,
}

/// Maps an engine rejection to the `(http_status, message)` the client sees,
/// matching the direct (non-Raft) write path's status codes.
pub fn engine_error_status(e: &nanobpmn_engine_core::EngineError) -> (u16, String) {
    use nanobpmn_engine_core::EngineError as E;
    match e {
        E::ProcessNotFound { process_id } => {
            (400, format!("No deployed process with id '{process_id}'."))
        }
        E::JobNotFound { job_key } => (404, format!("No job with key {job_key}.")),
        E::DecisionNotFound { .. } => (404, e.to_string()),
        E::JobNotActive { job_key } => (409, format!("Job {job_key} is not active.")),
        E::JobNotActivated { job_key } => (409, format!("Job {job_key} has not been activated.")),
        E::JobLeaseMismatch { .. } => (409, e.to_string()),
        E::JobUpdateInvalid { .. } => (400, e.to_string()),
        E::AgentInstanceNotFound { .. }
        | E::AgentInstanceElementInstanceInactive { .. }
        | E::AgentInstanceJobNotActive { .. }
        | E::AgentInstanceJobLeaseMismatch { .. } => (404, e.to_string()),
        E::AgentInstanceAlreadyExists { .. }
        | E::AgentInstanceActiveWriter { .. }
        | E::AgentInstanceConflict { .. } => (409, e.to_string()),
        E::AgentHistoryInvalid { .. }
        | E::AgentInstanceElementNotEligible { .. }
        | E::AgentInstanceMissingAgentDefinition { .. }
        | E::AgentInstanceJobRequiredForHistory { .. }
        | E::AgentInstanceJobElementMismatch { .. }
        | E::AgentInstanceOwnershipMismatch { .. }
        | E::AgentInstanceStatusNotSettable { .. }
        | E::AgentInstanceAlreadyCompleted { .. }
        | E::AgentInstanceLimitExceeded { .. } => (400, e.to_string()),
        // Durable REST/operator mutations routed through
        // `ServerImpl::apply_partition_command` surface their engine rejections
        // here, so every by-key mutation command's error maps to its HTTP status
        // in ONE place (the raft propose path and the direct local apply share
        // this mapping — no per-handler drift). #1306.
        E::InstanceNotFound { .. } => (404, e.to_string()),
        // Off-heap state transiently unreadable (#1388): retryable, never 404.
        E::InstanceUnavailable { .. } => (503, e.to_string()),
        // The suspend/resume contracts (spec/process-instances.yaml) define a
        // wrong-state transition as 409 Conflict, not 400: the instance exists
        // but its (terminal) state conflicts with the requested transition.
        E::InstanceTransitionInvalid { .. } => (409, e.to_string()),
        E::IncidentNotFound { .. } => (404, e.to_string()),
        E::IncidentNotResolvable { .. } => (409, e.to_string()),
        E::ScopeNotFound { .. } => (400, e.to_string()),
        E::UserTaskNotFound { .. } => (404, e.to_string()),
        E::UserTaskNotActive { .. } => (409, e.to_string()),
        E::UserTaskAlreadyAssigned { .. } => (409, e.to_string()),
        E::AdHocSubProcessNotFound { .. } | E::AdHocUnknownElement { .. } => (404, e.to_string()),
        E::AdHocNoActivationTargets { .. } => (400, e.to_string()),
        // Instance migration: an unknown target definition is 404 (like an
        // unknown instance above); an invalid mapping is 400; a migration the
        // engine rejects for the instance's live state is 409.
        E::TargetProcessDefinitionNotFound { .. } => (404, e.to_string()),
        E::DuplicateMappingSourceElement { .. }
        | E::MappingSourceElementNotFound { .. }
        | E::MappingTargetElementNotFound { .. } => (400, e.to_string()),
        E::UnmappedActiveElement { .. }
        | E::MappedElementTypeChanged { .. }
        | E::MigratedParallelJoinArityChanged { .. }
        | E::MigratedJoinFlowMissing { .. }
        | E::MigratedGatewayRouteMissing { .. }
        | E::UnsupportedMigration { .. } => (409, e.to_string()),
        // Process-instance modification / migration rejection surface (#1374),
        // Zeebe parity: a malformed or unresolvable instruction is
        // INVALID_ARGUMENT (400); an instance in the wrong state and a
        // migration precondition the live instance violates are INVALID_STATE
        // (409). (An unknown instance is `InstanceNotFound` → 404 above.)
        E::ElementNotFound { .. }
        | E::ElementInstanceNotFound { .. }
        | E::ModificationInstructionNotYetSupported { .. }
        | E::ModificationInstructionInvalid { .. }
        | E::AmbiguousFlowScope { .. }
        | E::AncestorScopeNotFound { .. }
        | E::AncestorNotAnAncestor { .. }
        | E::ElementNotActivatable { .. } => (400, e.to_string()),
        E::InstanceNotActive { .. }
        | E::MigrationFlowScopeChanged { .. }
        | E::MigrationMultiInstanceMismatch { .. }
        | E::MigrationCatchEventDetached { .. }
        | E::MigrationCatchEventsMerged { .. }
        // A target catch event whose subscription expression fails to evaluate
        // (Zeebe `ProcessInstanceMigrationCatchEventBehaviour`, INVALID_STATE).
        | E::MigrationCatchEventSubscriptionFailed { .. } => (409, e.to_string()),
        // A start event whose message-name / timer expression cannot be
        // evaluated at deploy time (Zeebe INVALID_ARGUMENT, #1398).
        E::StartEventExpressionInvalid { .. } => (400, e.to_string()),
        other => (500, other.to_string()),
    }
}

/// Maps an engine rejection to the `(http_status, message)` the client sees,
/// applying any **command-specific** status contract on top of the shared
/// [`engine_error_status`] mapping.
///
/// A handful of engine errors are status-polymorphic across endpoints, so a
/// command-agnostic mapping cannot serve every caller. `AdHocUnknownElement` is
/// the canonical case: for `ActivateAdHocActivities` an unknown container or
/// element id is "not found" (404), but for `CompleteJob` (an agentic ad-hoc
/// completion whose `JobResult` referenced an unknown activation target) it is
/// invalid data (400). The non-Raft direct paths encode these per-endpoint (see
/// `complete_job_impl`), so the replicated path must key on the command kind
/// ([`Command::kind`]) too — otherwise the same rejection on a replicated
/// `CompleteJob` is mis-reported as `Status404_TheJobWithTheGivenKeyWasNotFound`,
/// falsely claiming the job is missing. #1308.
pub fn engine_error_status_for(kind: &str, e: &nanobpmn_engine_core::EngineError) -> (u16, String) {
    use nanobpmn_engine_core::EngineError as E;
    if kind == "complete_job" {
        // The `CompleteJob` endpoint's direct (non-Raft) contract for its
        // ad-hoc-completion and business-id rejections (`complete_job_impl`):
        // an unknown/contradictory ad-hoc activation target and an invalid
        // business id are 400 (invalid data), a business-id conflict is 409.
        // The shared mapping would otherwise surface these as 404 / 500.
        match e {
            E::AdHocUnknownElement { .. }
            | E::AdHocActivateWithCompletion { .. }
            | E::BusinessIdInvalid { .. } => return (400, e.to_string()),
            E::BusinessIdOnChildInstance { .. } | E::BusinessIdAlreadyAssigned { .. } => {
                return (409, e.to_string());
            }
            _ => {}
        }
    }
    engine_error_status(e)
}

/// In-memory Raft log store (v2 `RaftLogStorage`). Holds the log entries, the
/// persisted vote, and the committed marker in memory.
///
/// NOTE (milestone A): in-memory means the *replicated log* is volatile — only
/// the applied engine state is durable (via the state machine's `Commit`). A
/// later milestone swaps this for a `Journal`-backed store so the log itself is
/// crash-durable; the trait seam here is exactly that swap point.
#[derive(Clone, Default)]
pub struct MemLogStore {
    inner: Arc<Mutex<MemLogInner>>,
}

#[derive(Default)]
struct MemLogInner {
    log: BTreeMap<u64, Entry<RaftConfig>>,
    last_purged: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
}

/// Normalize an openraft log-entry range request into an inclusive `lo..=hi`,
/// or `None` when the request is empty.
///
/// openraft 0.9.24 can hand the log reader a **degenerate** range (`start >
/// end`, or an exclusive end of `0`) during a promoted single-voter group's
/// rejoin/snapshot-send race: the leader keeps applying at full rate while it
/// retries an `InstallSnapshot` to the returning member, and the apply loop's
/// `(last_applied, committed]` window can momentarily invert. Passing such a
/// range straight to [`BTreeMap::range`] **panics** ("range start is greater
/// than range end in BTreeMap"). That panic used to abort the whole node (every
/// partition it hosts) under the old fatal-panic build; even now it would
/// needlessly unwind this partition's raft task. Clamping to an empty result
/// here keeps a single partition's storage read total, so a transient openraft
/// edge case can never destabilize the process.
pub(crate) fn clamp_log_range<RB: RangeBounds<u64>>(
    range: &RB,
) -> Option<std::ops::RangeInclusive<u64>> {
    use std::ops::Bound;
    let lo = match range.start_bound() {
        Bound::Included(&i) => i,
        Bound::Excluded(&i) => i.checked_add(1)?,
        Bound::Unbounded => u64::MIN,
    };
    let hi = match range.end_bound() {
        Bound::Included(&i) => i,
        Bound::Excluded(&i) => i.checked_sub(1)?, // exclusive end of 0 => empty
        Bound::Unbounded => u64::MAX,
    };
    if lo <= hi { Some(lo..=hi) } else { None }
}

impl RaftLogReader<RaftConfig> for MemLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RaftConfig>>, StorageError<NodeId>> {
        // Guard against a degenerate (inverted/empty) range before touching the
        // BTreeMap, which would otherwise panic and unwind this partition's raft
        // task (losing the partition on this node). See `clamp_log_range`.
        let Some(bounds) = clamp_log_range(&range) else {
            return Ok(Vec::new());
        };
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok(inner.log.range(bounds).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<RaftConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<RaftConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let last_log_id = inner
            .log
            .values()
            .next_back()
            .map(|e| e.log_id)
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap_or_else(|e| e.into_inner()).vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<RaftConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<RaftConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            for entry in entries {
                inner.log.insert(entry.log_id.index, entry);
            }
        }
        // In-memory: the write is immediately "durable", so report completion now.
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Remove everything from `log_id.index` onward (inclusive).
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let _removed = inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Drop everything up to and including `log_id.index`, keep the rest.
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.last_purged = Some(log_id);
        inner.log = inner.log.split_off(&(log_id.index + 1));
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .committed)
    }
}

/// File-backed [`SnapshotData`](RaftConfig::SnapshotData) so a partition snapshot
/// is serialized to / streamed from disk instead of being materialized as a
/// `Cursor<Vec<u8>>` in RAM. This keeps snapshot build, cache and transfer memory
/// bounded (a handful of chunk buffers) rather than holding a full multi-gigabyte
/// copy of every resident variable — *twice*, once for the returned reader and
/// once for the cached `current_snapshot` — per partition. That eager double copy
/// (`serde_json::to_vec` + `data.clone()`) was the "fat snapshot" the lean design
/// removes. The `path` rides along with the tokio [`File`](tokio::fs::File) so the
/// state machine can persist / reopen the exact file openraft hands back through
/// [`install_snapshot`](RaftStateMachine::install_snapshot).
///
/// All three async traits simply delegate to the inner file, which is `Unpin`, so
/// the wrapper is `Unpin` too and can be pin-projected with [`Pin::new`].
pub struct SnapshotFile {
    file: tokio::fs::File,
    path: PathBuf,
    /// Set on the RECEIVE path only (#1488 review): the in-flight registration
    /// for this `incoming-*.tmp` file, taken by `install_snapshot` so the file
    /// is registered from its creation until its promotion rename — a
    /// concurrent build's reap can never delete it mid-receive.
    in_flight: Option<InFlightSnapshot>,
}

impl tokio::io::AsyncRead for SnapshotFile {
    fn poll_read(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &mut tokio::io::ReadBuf<'_>,
    ) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_read(cx, buf)
    }
}

impl tokio::io::AsyncWrite for SnapshotFile {
    fn poll_write(
        self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::io::Result<usize>> {
        Pin::new(&mut self.get_mut().file).poll_write(cx, buf)
    }

    fn poll_flush(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_flush(cx)
    }

    fn poll_shutdown(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<()>> {
        Pin::new(&mut self.get_mut().file).poll_shutdown(cx)
    }
}

impl tokio::io::AsyncSeek for SnapshotFile {
    fn start_seek(self: Pin<&mut Self>, position: std::io::SeekFrom) -> std::io::Result<()> {
        Pin::new(&mut self.get_mut().file).start_seek(position)
    }

    fn poll_complete(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<std::io::Result<u64>> {
        Pin::new(&mut self.get_mut().file).poll_complete(cx)
    }
}

/// A persisted snapshot: the metadata plus the on-disk path of the serialized
/// [`EngineSnapshot`] that reconstructs the engine directly (state-based, not
/// event-replay — its size tracks the live working set rather than growing with
/// every command ever applied, so the Raft log can be compacted without unbounded
/// memory growth). The body lives on disk (not a cached `Vec<u8>`) so holding the
/// current snapshot for the follower catch-up path costs a path, not a full copy
/// of the state in RAM.
#[derive(Debug, Clone)]
struct StoredSnapshot {
    meta: SnapshotMeta<NodeId, BasicNode>,
    path: PathBuf,
}

/// The durable pointer to a partition's current snapshot, persisted next to the
/// snapshot `.bin` as `current-snapshot.json`.
///
/// # Why this exists (the rejoin-brick bug)
///
/// By the Raft model the log is the source of truth, and boot used to rebuild
/// engine state by replaying the *full* durable log — so it unconditionally
/// deleted every on-disk snapshot as dead. That is only sound while the log is
/// never compacted. But openraft snapshots then **purges** the log (persisting a
/// `last_purged` marker and dropping every covered entry). After a purge the
/// snapshot is the ONLY source for the `[0, last_purged]` prefix; deleting it on
/// the next boot left openraft with a `last_purged` marker but no snapshot and no
/// entries below it, so hosting the partition failed with a degenerate
/// `expected [0, N), got [None, None)` log read and the partition never formed
/// its group (received zero traffic thereafter).
///
/// The pointer is written atomically once the `.bin` is fsync'd and **before**
/// openraft is allowed to purge the log the snapshot subsumes, so a restart can
/// always restore the exact state a subsequent purge relied on.
#[derive(Serialize, Deserialize)]
struct PersistedSnapshotPtr {
    /// File name (not the full path) of the current snapshot `.bin`, resolved
    /// against the snapshot dir so the pointer survives a data-dir move.
    file: String,
    last_log_id: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    snapshot_id: String,
}

fn snapshot_ptr_path(dir: &Path) -> PathBuf {
    dir.join("current-snapshot.json")
}

/// The generation a snapshot file name belongs to (#1473): the `g<N>` infix a
/// member stamps into every artifact it publishes (`snap-g<N>-<idx>.bin` /
/// `snap-installed-g<N>-<idx>.bin`). A member reopening a durable snapshot dir
/// continues in generation `N+1`, so two generations of one partition never
/// target the same file name on the shared dir. A pre-#1473 (un-tagged) name
/// parses as generation 0, so the first post-upgrade member continues at 1.
fn snapshot_generation_of(file_name: &str) -> u64 {
    let body = file_name
        .strip_prefix("snap-installed-")
        .or_else(|| file_name.strip_prefix("snap-"))
        .unwrap_or(file_name);
    body.strip_prefix('g')
        .and_then(|rest| rest.split('-').next())
        .and_then(|n| n.parse::<u64>().ok())
        .unwrap_or(0)
}

/// Atomically persist the current-snapshot pointer (temp write + fsync + rename +
/// directory fsync) so it is crash-durable before the caller returns.
fn write_snapshot_ptr(dir: &Path, stored: &StoredSnapshot) -> std::io::Result<()> {
    let file = stored
        .path
        .file_name()
        .and_then(|n| n.to_str())
        .ok_or_else(|| {
            std::io::Error::new(
                std::io::ErrorKind::InvalidInput,
                "snapshot path has no file name",
            )
        })?
        .to_string();
    let ptr = PersistedSnapshotPtr {
        file,
        last_log_id: stored.meta.last_log_id,
        last_membership: stored.meta.last_membership.clone(),
        snapshot_id: stored.meta.snapshot_id.clone(),
    };
    let bytes = serde_json::to_vec(&ptr)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    let tmp = dir.join(format!("current-snapshot.json.tmp-{}", std::process::id()));
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(&bytes)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, snapshot_ptr_path(dir))?;
    // fsync the directory so the rename (and any preceding unlink) is durable.
    if let Ok(dirf) = std::fs::File::open(dir) {
        let _ = dirf.sync_all();
    }
    Ok(())
}

/// Read the durable current-snapshot pointer, returning the [`StoredSnapshot`] it
/// names iff the referenced `.bin` is physically present.
fn read_snapshot_ptr(dir: &Path) -> Option<StoredSnapshot> {
    let bytes = std::fs::read(snapshot_ptr_path(dir)).ok()?;
    let ptr: PersistedSnapshotPtr = serde_json::from_slice(&bytes).ok()?;
    let path = dir.join(&ptr.file);
    if !path.is_file() {
        return None;
    }
    Some(StoredSnapshot {
        meta: SnapshotMeta {
            last_log_id: ptr.last_log_id,
            last_membership: ptr.last_membership,
            snapshot_id: ptr.snapshot_id,
        },
        path,
    })
}

/// Whether hosting `log_dir`'s durable on-disk log would hit a **purge-hole** —
/// the case where a node that was down longer than the leader's log-retention
/// window rejoins with a committed index beyond what its local snapshot covers,
/// while the log entries needed to replay that gap have already been purged.
///
/// # Why this exists (purge-hole → snapshot fallback, issue #111)
///
/// On boot openraft's `get_initial_state` replays `(last_applied, committed]` from
/// the log to rebuild the state machine up to the durable commit point. `apply`'s
/// snapshot restores `last_applied`; the durable store filters every entry at or
/// below `last_purged` on open. So when `last_purged >= last_applied.next_index()`
/// **and** `committed > last_applied`, the very first entry the reapply needs is
/// physically gone: openraft raises a defensive `LogIndexNotFound (want:N …)` and
/// the partition **fails to host** (a rejoining node then silently runs its owned
/// groups only, dropping its replica groups — degraded RF + a peer AppendEntries
/// storm). The Raft-correct recovery is to discard the unusable local log and
/// **install a fresh snapshot from the current leader**, so the caller hosts the
/// member as an empty receiver (`log_dir = None`) instead of resuming on-disk.
///
/// This is a server-side detection that maps cleanly onto openraft 0.10's
/// `loosen-follower-log-revert` + app-side snapshot transport (issue #111): once
/// migrated, the follower may revert to an empty log without panicking the leader,
/// and this helper becomes deletable.
///
/// Returns `false` (host on-disk normally) when there is no committed marker, when
/// the snapshot already covers the committed point (no gap to replay), or when the
/// retained log tail still holds the reapply range (the common brief-restart case).
pub fn durable_log_has_purge_hole(log_dir: &Path) -> bool {
    let (committed, last_purged) = crate::raft_logstore::peek_committed_and_purged(log_dir);
    // A fresh/empty durable dir (nothing committed) hosts normally.
    let Some(committed) = committed else {
        return false;
    };
    // The snapshot `apply` will restore covers `[.., last_applied]`.
    let last_applied =
        read_snapshot_ptr(&log_dir.join("snapshots")).and_then(|s| s.meta.last_log_id);
    // Snapshot already at/after the commit point ⇒ boot reapply is a no-op ⇒ no hole.
    if last_applied
        .map(|a| a.index >= committed.index)
        .unwrap_or(false)
    {
        return false;
    }
    // The reapply needs `[last_applied.next_index() .. committed]`; its first entry
    // is `last_applied.index + 1` (or `0` when there is no snapshot). Those entries
    // are physically absent iff the durable purge marker has advanced to or past
    // that first index (the store drops everything `<= last_purged` on open).
    let needed_first = last_applied.map(|a| a.index + 1).unwrap_or(0);
    last_purged
        .map(|p| p.index >= needed_first)
        .unwrap_or(false)
}

/// The replication state journaled in each [`Event::RaftEntryApplied`] record
/// (#1405): the applied log id and the membership in effect after it. Opaque to
/// engine-core (carried as a JSON string), decoded only here.
#[derive(Serialize, Deserialize)]
struct JournaledRaftState {
    log_id: LogId<NodeId>,
    membership: StoredMembership<NodeId, BasicNode>,
}

/// Serializes the [`JournaledRaftState`] for `log_id` + `membership`.
fn journaled_raft_state(
    log_id: LogId<NodeId>,
    membership: &StoredMembership<NodeId, BasicNode>,
) -> std::io::Result<String> {
    serde_json::to_string(&JournaledRaftState {
        log_id,
        membership: membership.clone(),
    })
    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Metadata held by the Raft state machine: the last applied log id and
/// membership. The materialized engine state itself lives on the partition's
/// [`DeepthiHandle`] (driven by [`apply`](RaftStateMachine::apply)) and is
/// captured on demand for snapshots, so the state machine retains no event
/// history of its own.
struct SmMeta {
    partition_id: u64,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

/// The Raft state machine for one partition. Committed commands are applied to
/// the partition's [`DeepthiHandle`] — the *same* single-writer engine actor the
/// rest of the server reads, dispatches jobs from, and runs timers on — so the
/// replicated log and the served state share one materialized copy. Wrapped in
/// an `Arc` so openraft can share it with the snapshot builder.
pub struct PartitionStateMachine {
    /// The partition's engine actor: `apply` forwards each committed command to
    /// it. Held outside the metadata `Mutex` so `apply` can `.await` the engine
    /// round-trip without holding a std lock across the await point.
    engine: DeepthiHandle,
    inner: Mutex<SmMeta>,
    snapshot_idx: AtomicU64,
    /// Monotonic sequence for uniquely naming in-flight received snapshot files
    /// (one partition can receive successive snapshots over its lifetime).
    recv_idx: AtomicU64,
    current_snapshot: Mutex<Option<StoredSnapshot>>,
    /// Snapshot files a build or install is still writing / promoting and has
    /// not yet published (#1488). [`reap_superseded_snapshots`] never removes a
    /// path in this set, so an install's prompt reap cannot delete the file a
    /// concurrent DETACHED `build_snapshot` is mid-writing (or an install's
    /// promoted-but-unpublished file) out from under it. Registered via
    /// [`InFlightSnapshot`] BEFORE the file exists, removed on drop.
    in_flight_snapshots: Arc<Mutex<std::collections::HashSet<PathBuf>>>,
    /// Directory holding this partition's snapshot files (both the current cached
    /// snapshot and transient incoming ones). Created on construction.
    snapshot_dir: PathBuf,
    /// This node's id, compared against [`leader`](Self::leader) so `apply` can
    /// tell whether it is currently the leader of this partition.
    node_id: NodeId,
    /// Whether this member is *eligible* to evict terminal instances in `apply`:
    /// `true` only for a partition this node REPLICATES but does not statically
    /// own (a follower under RF>1), which has no read-model exporter to drive
    /// eviction. Combined with the dynamic leadership check below.
    evict_eligible: bool,
    /// The partition's current Raft leader node id (`u64::MAX` = none), kept live
    /// by a metrics watcher spawned in [`bootstrap_member`](RaftPartition::bootstrap_member).
    ///
    /// Eviction is gated on `evict_eligible && leader != node_id`: a follower
    /// reclaims each instance's hot-state shell the moment it turns terminal
    /// (it has no exporter, so otherwise terminal shells accumulate without
    /// bound — the RF>1 leak). But the instant this member *becomes* the leader
    /// (elected or promoted after a failover) it STOPS evicting, because it now
    /// serves reads/status straight from its engine — exactly the ADR-0012
    /// reason a statically-owned leader keeps the shell resident (its exporter
    /// drives eviction there instead).
    leader: Arc<AtomicU64>,
    /// Test-only barrier (#1405): signalled by an adoption the instant it is
    /// about to wait on the checkpoint permit, so a test can deterministically
    /// observe that adoption reached the wait BEFORE touching the engine.
    #[cfg(test)]
    adoption_permit_waiting: Arc<tokio::sync::Notify>,
    /// The engine lifecycle epoch this member was constructed in (#1473). Every
    /// engine-touching operation (apply, snapshot capture/build/install) enters
    /// the engine's single lifecycle lock SHARED with this epoch
    /// ([`DeepthiHandle::lifecycle_enter`]) and is refused once the epoch has
    /// been retired by [`RaftPartition::retire`] — one lock for the whole class
    /// of "predecessor work races retirement / engine reuse", instead of a
    /// bespoke fence per operation.
    epoch: u64,
    /// Monotonic generation of this state machine's snapshot file names
    /// (#1473). Derived from the durable snapshot pointer's file name at
    /// construction, so a member that REOPENS a durable snapshot directory
    /// continues in a strictly newer generation than every member that ran
    /// before it — a late predecessor build/install can then never collide
    /// with (or be mistaken for) a successor's artifact on the shared dir.
    generation: u64,
}

impl PartitionStateMachine {
    fn new(
        engine: DeepthiHandle,
        partition_id: u64,
        snapshot_dir: PathBuf,
        node_id: NodeId,
        evict_eligible: bool,
        leader: Arc<AtomicU64>,
    ) -> std::io::Result<Self> {
        std::fs::create_dir_all(&snapshot_dir)?;
        // Restore from the durable current-snapshot pointer if one is present.
        // Once the log has been compacted (openraft persists a `last_purged`
        // marker and drops the covered entries), the snapshot is the ONLY source
        // for the purged prefix; deleting it here — as this used to
        // unconditionally do — bricks the partition on restart (it fails to host
        // with a degenerate `[0, N)` log read; see [`PersistedSnapshotPtr`]). So
        // keep the pointed-at snapshot, adopt its applied metadata, and garbage
        // collect only the OTHER (stale / orphan `incoming-*`) snapshot files.
        let restored = read_snapshot_ptr(&snapshot_dir);
        let keep = restored.as_ref().map(|s| s.path.clone());
        if let Ok(entries) = std::fs::read_dir(&snapshot_dir) {
            for entry in entries.flatten() {
                let path = entry.path();
                if keep.as_deref() == Some(path.as_path()) {
                    continue;
                }
                let ours = path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(|n| n.starts_with("snap-") || n.starts_with("incoming-"))
                    .unwrap_or(false);
                if ours {
                    let _ = std::fs::remove_file(&path);
                }
            }
        }
        if restored.is_none() {
            // No durable snapshot: the pointer (if any) is dead — drop it so a
            // later successful build writes a clean one.
            let _ = std::fs::remove_file(snapshot_ptr_path(&snapshot_dir));
        }
        let (last_applied, last_membership) = restored
            .as_ref()
            .map(|s| (s.meta.last_log_id, s.meta.last_membership.clone()))
            .unwrap_or_else(|| (None, StoredMembership::default()));
        // The generation this member's snapshot file names continue in (#1473):
        // one past the restored artifact's generation, so a member reopening a
        // durable snapshot dir never reuses a name a predecessor generation may
        // still have in flight. A fresh dir (or a pre-#1473 un-tagged pointer)
        // starts at generation 1.
        // Bind this member to the engine's CURRENT lifecycle epoch (#1473): a
        // successor is always constructed after its predecessor's retirement
        // bumped the epoch, so the two can never both be admitted.
        let epoch = engine.lifecycle_epoch();
        let generation = restored
            .as_ref()
            .and_then(|s| {
                s.path
                    .file_name()
                    .and_then(|n| n.to_str())
                    .map(snapshot_generation_of)
            })
            .map(|g| g + 1)
            .unwrap_or(1);
        Ok(Self {
            engine,
            inner: Mutex::new(SmMeta {
                partition_id,
                last_applied,
                last_membership,
            }),
            snapshot_idx: AtomicU64::new(0),
            recv_idx: AtomicU64::new(0),
            current_snapshot: Mutex::new(restored),
            in_flight_snapshots: Arc::default(),
            snapshot_dir,
            node_id,
            evict_eligible,
            leader,
            #[cfg(test)]
            adoption_permit_waiting: Arc::default(),
            epoch,
            generation,
        })
    }

    /// The partition this state machine replicates.
    fn partition_id(&self) -> u64 {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .partition_id
    }

    /// A unique per-process, per-partition snapshot directory under the system
    /// temp dir, for in-memory deployments and tests that have no durable log dir
    /// to anchor snapshots to.
    fn temp_snapshot_dir(partition_id: u64) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .map(|d| d.as_nanos())
            .unwrap_or(0);
        std::env::temp_dir().join(format!(
            "{RAFT_SNAPSHOT_DIR_PREFIX}{}-p{partition_id}-{nanos}",
            std::process::id()
        ))
    }

    /// What [`RaftPartition`] needs to retire this member (#1473): the engine
    /// whose lifecycle lock it takes, the epoch this member is bound to, and
    /// the snapshot directory (reclaimed when volatile).
    fn retirement_handles(&self) -> (DeepthiHandle, u64, PathBuf) {
        (self.engine.clone(), self.epoch, self.snapshot_dir.clone())
    }

    /// Enters the engine lifecycle lock SHARED for this member (#1473), or
    /// `None` when the member has been retired. Hold the guard for the whole
    /// engine-touching operation: retirement cannot complete while it is held,
    /// and the epoch cannot change under it.
    async fn enter_lifecycle(&self) -> Option<LifecycleShared> {
        self.engine.lifecycle_enter(self.epoch).await
    }

    /// Whether this member's lifecycle epoch has been retired (non-blocking
    /// probe; authoritative checks go through [`Self::enter_lifecycle`]).
    fn is_retired(&self) -> bool {
        self.engine.lifecycle_epoch() != self.epoch
    }

    /// The typed refusal every lifecycle-fenced operation returns once its
    /// member is retired (#1473).
    fn retired_error(
        subject: ErrorSubject<NodeId>,
        verb: ErrorVerb,
        what: &str,
    ) -> StorageError<NodeId> {
        StorageError::from_io_error(
            subject,
            verb,
            std::io::Error::other(format!(
                "refusing to {what}: the state machine was retired (its engine lifecycle \
                 epoch ended) (#1473)"
            )),
        )
    }

    /// [`new`](Self::new) with a fresh temp snapshot directory. Used by the
    /// in-memory (volatile-log) bootstraps and the unit tests. Never eligible to
    /// evict (owned/serving semantics), so the leader flag is inert.
    fn new_temp(engine: DeepthiHandle, partition_id: u64) -> std::io::Result<Self> {
        Self::new(
            engine,
            partition_id,
            Self::temp_snapshot_dir(partition_id),
            0,
            false,
            Arc::new(AtomicU64::new(u64::MAX)),
        )
    }

    /// Test-only continuation member (#1488 review regression): a second SM on
    /// the SAME snapshot dir that shares this member's receive epoch — its
    /// `recv_idx` (so its receive file names continue this member's sequence)
    /// and its in-flight set (so a receive this member has in flight is
    /// registered for the continuation's constructor sweep too). Models a
    /// detached build openraft queued on a rebuilt member while a receive is
    /// still streaming.
    #[cfg(test)]
    fn new_continuation_for_test(&self, engine: DeepthiHandle) -> std::io::Result<Self> {
        let mut member = Self::new(
            engine,
            self.partition_id(),
            self.snapshot_dir.clone(),
            self.node_id,
            false,
            Arc::clone(&self.leader),
        )?;
        member.recv_idx = AtomicU64::new(self.recv_idx.load(Ordering::Relaxed));
        member.in_flight_snapshots = Arc::clone(&self.in_flight_snapshots);
        Ok(member)
    }

    /// Restore the engine to the state captured by the durable current snapshot,
    /// if any. Called once at boot (from
    /// [`bootstrap_member`](RaftPartition::bootstrap_member)) BEFORE the openraft
    /// [`Raft`](openraft::Raft) is constructed, so the state machine already
    /// carries the snapshot's applied metadata (adopted in [`new`](Self::new))
    /// and openraft only has to replay the post-snapshot log tail on top. A no-op
    /// when there is no durable snapshot (the unpurged log replays in full, the
    /// original recovery path).
    async fn restore_from_current_snapshot(&self) -> anyhow::Result<()> {
        let path = {
            let guard = self
                .current_snapshot
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            match guard.as_ref() {
                Some(s) => (s.path.clone(), s.meta.clone()),
                None => return Ok(()),
            }
        };
        let (path, meta) = path;
        let captured: nanobpmn_engine_core::EngineSnapshot =
            tokio::task::spawn_blocking(move || -> std::io::Result<_> {
                let f = std::fs::File::open(&path)?;
                nano_server_storage::snapstream::read_streaming_engine_snapshot_from(f)
            })
            .await??;
        self.adopt_engine_snapshot(captured, &meta).await
    }

    /// The ONE snapshot-adoption routine (#1405), shared by boot-time adoption
    /// ([`restore_from_current_snapshot`](Self::restore_from_current_snapshot))
    /// and runtime `install_snapshot`: rebuilds the engine from `captured` IN
    /// PLACE (keeping the journal's exporter / partition / var-store / spill
    /// wiring) and then makes that state the journal's durable recovery base
    /// (see below).
    /// Adopting without checkpointing would leave a journal that lacks the
    /// snapshot's state; once a later entry is journaled, a restart prefers that
    /// journal and silently loses every snapshot-only instance.
    ///
    /// The adopted state's applied record is DERIVED from the snapshot's Raft
    /// `meta` (the single source of truth), never trusted from the engine body:
    /// a legacy snapshot body decodes with `raft_applied = None`, and
    /// checkpointing that would let a crash before the snapshot pointer is
    /// published boot with `last_applied = None` and re-execute committed
    /// creates.
    async fn adopt_engine_snapshot(
        &self,
        mut captured: nanobpmn_engine_core::EngineSnapshot,
        meta: &SnapshotMeta<NodeId, BasicNode>,
    ) -> anyhow::Result<()> {
        captured.raft_applied = match meta.last_log_id {
            Some(log_id) => Some((
                log_id.index,
                journaled_raft_state(log_id, &meta.last_membership)?,
            )),
            None => None,
        };
        // The checkpoint permit is acquired BEFORE the engine is restored and
        // held through the base publication, and restore + checkpoint run in
        // ONE actor turn: a periodic checkpoint can therefore never capture the
        // adopted state (and overwrite its variables) between the restore and
        // the base publication, which a crash would leave as an old snapshot
        // paired with adopted variables.
        let permit = self.adoption_checkpoint_permit().await;
        self.engine
            .with(move |journal| {
                journal.restore_engine_from_snapshot(captured);
                journal.checkpoint_base(permit.as_ref())
            })
            .await
            .map_err(|e| anyhow::anyhow!("checkpointing the adopted Raft snapshot failed: {e}"))
    }

    /// Selects the NEWEST consistent boot state for a durable member (#1405):
    /// the journal-rebuilt engine (applied index = its last
    /// [`Event::RaftEntryApplied`] record) or the durable Raft snapshot
    /// (applied index = its meta), together with the matching applied/membership
    /// metadata. The engine was rebuilt from the journal, so when the journal is
    /// at or ahead of the snapshot restoring the OLDER snapshot over it would
    /// discard the journaled suffix — and openraft would then re-execute (and
    /// re-journal) that suffix, running every non-idempotent create twice.
    /// Only a snapshot strictly ahead of the journal (or a journal with no
    /// applied-entry record at all) is restored. The single canonical boot-state
    /// rule for every durable bootstrap path.
    async fn restore_newest_boot_state(&self) -> anyhow::Result<()> {
        let journaled = self.journaled_applied().await?;
        let snapshot = self
            .current_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .and_then(|s| s.meta.last_log_id);
        match (journaled, snapshot) {
            (Some(journal), Some(snap)) if snap > journal.log_id => {
                // The snapshot is strictly newer: install it (its metadata was
                // adopted in `new`); openraft replays only the tail after it.
                self.restore_from_current_snapshot().await
            }
            (Some(_), _) => self.seed_last_applied_from_journal().await,
            (None, Some(_)) => self.restore_from_current_snapshot().await,
            (None, None) => Ok(()),
        }
    }

    /// Acquires the journal's checkpoint permit for an adoption (#1405), off
    /// the engine actor and before entering it (the periodic checkpoint takes
    /// it the same way), so adoption is serialized with any in-flight periodic
    /// capture -> publish cycle without the two deadlocking on the actor.
    async fn adoption_checkpoint_permit(
        &self,
    ) -> Option<nano_server_storage::seglog::CheckpointPermit> {
        let seg = self.engine.with(|journal| journal.seg_shared()).await;
        match seg {
            Some(seg) => {
                #[cfg(test)]
                self.adoption_permit_waiting.notify_one();
                Some(seg.checkpoint_permit().await)
            }
            None => None,
        }
    }

    /// Decodes the journal's last [`Event::RaftEntryApplied`] record, if any.
    /// An undecodable record fails closed rather than guessing.
    async fn journaled_applied(&self) -> anyhow::Result<Option<JournaledRaftState>> {
        let Some((index, raft_state)) = self.engine.with(|journal| journal.raft_applied()).await
        else {
            return Ok(None);
        };
        serde_json::from_str(&raft_state).map(Some).map_err(|e| {
            anyhow::anyhow!("undecodable Raft applied-entry record at index {index}: {e}")
        })
    }

    /// Seeds `last_applied` (and the membership it established) from the
    /// journal's last [`Event::RaftEntryApplied`] record (#1405), when it is
    /// AHEAD of what the state machine already knows (no Raft snapshot, or an
    /// older one). The engine was rebuilt from that journal, so every entry at
    /// or below the record is already applied; without the seed openraft would
    /// re-apply them from `last_applied = None` and execute non-idempotent
    /// commands twice. An undecodable record fails closed (bootstrap errors)
    /// rather than guessing.
    async fn seed_last_applied_from_journal(&self) -> anyhow::Result<()> {
        let Some(recorded) = self.journaled_applied().await? else {
            return Ok(());
        };
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if Some(recorded.log_id) > inner.last_applied {
            tracing::info!(
                partition = inner.partition_id,
                last_applied = %recorded.log_id,
                "raft: seeding last_applied from the journal's applied-entry record (#1405)"
            );
            inner.last_applied = Some(recorded.log_id);
            inner.last_membership = recorded.membership;
        }
        Ok(())
    }

    /// Test-only install variant (#1488 review regression): runs the whole
    /// publish side (promote, durable pointer, size metric, reap, cache) but
    /// skips the engine adoption, so a test can interleave an install publish
    /// with a paused build on the SAME engine — the adoption's checkpoint
    /// permit is held by that build's capture path, and the state being
    /// adopted is that engine's own.
    #[cfg(test)]
    #[allow(
        clippy::result_large_err,
        reason = "test helper returning openraft's StorageError verbatim; the production install signature is fixed by the RaftStateMachine trait"
    )]
    async fn install_snapshot_skip_adoption_for_test(
        &self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError<NodeId>> {
        self.install_snapshot_inner(meta, snapshot, false).await
    }

    #[allow(
        clippy::result_large_err,
        reason = "internal split of install_snapshot, whose signature (and Err size) is fixed by the RaftStateMachine trait"
    )]
    async fn install_snapshot_inner(
        &self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<SnapshotFile>,
        adopt: bool,
    ) -> Result<(), StorageError<NodeId>> {
        // Engine lifecycle lock (#1473): held SHARED across the WHOLE install —
        // deserialize, publish the pointer, adopt into the engine. A retired
        // member is refused BEFORE deserialization or adoption (so an old
        // snapshot can never overwrite the engine a successor reuses), and
        // retirement cannot complete while an install is mid-flight. Lock
        // order: lifecycle → the snapshot publication lock (the publish) →
        // the journal's checkpoint permit (the adoption); the publication
        // lock is always released before the checkpoint permit is taken.
        let _lifecycle = self.enter_lifecycle().await.ok_or_else(|| {
            PartitionStateMachine::retired_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                "install a Raft snapshot",
            )
        })?;
        let SnapshotFile {
            file,
            path,
            in_flight,
        } = *snapshot;
        // Register the received `incoming-*.tmp` file in flight from here until
        // its promotion rename below (#1488 review): a concurrent build's reap
        // matches every `snap-`/`incoming-` artifact, so an unregistered
        // receive would be deleted out from under the promotion.
        let _received_in_flight = in_flight
            .unwrap_or_else(|| InFlightSnapshot::register(&self.in_flight_snapshots, &path));

        // Stream-deserialize the received chunked snapshot (#1337) from a
        // blocking task: read the control skeleton, then restore each instance
        // chunk-by-chunk, so the receive side never holds more than one chunk in
        // flight before it is folded into the rebuilt state. openraft leaves the
        // write cursor at the end, so rewind first.
        let std_file = file.into_std().await;
        let captured: nanobpmn_engine_core::EngineSnapshot = tokio::task::spawn_blocking(
            move || -> std::io::Result<nanobpmn_engine_core::EngineSnapshot> {
                use std::io::Seek;
                let mut f = std_file;
                f.seek(std::io::SeekFrom::Start(0))?;
                nano_server_storage::snapstream::read_streaming_engine_snapshot_from(f)
            },
        )
        .await
        .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?
        .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        // Promote the received file to the current snapshot (a rename within the
        // same dir — cheap, no re-serialize, no extra copy) and drop the old one.
        // The name is generation-stamped (#1473) so a late predecessor install
        // can never collide with the successor's artifacts on the reopened dir.
        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let current_path = self.snapshot_dir.join(format!(
            "snap-installed-g{}-{snapshot_idx}.bin",
            self.generation
        ));
        // In flight until published (#1488): a concurrent build's reap must not
        // delete the promoted-but-unpublished file.
        let _in_flight = InFlightSnapshot::register(&self.in_flight_snapshots, &current_path);
        std::fs::rename(&path, &current_path).map_err(|e| {
            StorageIOError::write_snapshot(
                Some(meta.signature()),
                &std::io::Error::new(
                    e.kind(),
                    format!("promote {path:?} -> {current_path:?}: {e}"),
                ),
            )
        })?;
        // fsync the promoted file, then record it as the durable current snapshot
        // BEFORE unlinking the one it replaces — an installed snapshot is just as
        // much a restart recovery point as a locally-built one, and the log store
        // will purge below it (see [`PersistedSnapshotPtr`]).
        if let Ok(f) = std::fs::File::open(&current_path) {
            let _ = f.sync_all();
        }
        // Serialize publication with reaping (#1488 review): the durable
        // pointer write, the current-snapshot cache update, and the
        // superseded-artifact reap run under the partition's snapshot
        // publication lock, so a concurrent build's sweep can never delete
        // the file this install just made the durable recovery point (the
        // in-flight guard alone cannot: it drops when this install returns,
        // while the build's sweep may run afterwards). The lock is dropped
        // BEFORE the adoption below — adoption takes the journal's checkpoint
        // permit, a different lock domain that must never be held together
        // with the publication lock.
        let publish_guard: SnapshotPublishGuard = self.engine.snapshot_publish_lock().await;
        // Never regress the published snapshot (#1488 review): if a newer
        // snapshot is already published, this install is stale — discard its
        // file and leave the pointer, cache and engine untouched.
        if published_snapshot_supersedes(&self.current_snapshot, meta).is_some() {
            let _ = std::fs::remove_file(&current_path);
            return Ok(());
        }
        let stored = StoredSnapshot {
            meta: meta.clone(),
            path: current_path,
        };
        write_snapshot_ptr(&self.snapshot_dir, &stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        // #1488: publish the installed snapshot's per-partition/per-replica size
        // (+ the oversize alert) and promptly reap every OTHER superseded
        // snapshot artifact on this follower — under a catch-up retry loop the
        // orphaned leftovers are multi-GB each (the #100 follower blowup) and
        // must not accumulate until the next process restart sweeps them.
        let partition_id = self.partition_id();
        let installed_bytes = std::fs::metadata(&stored.path)
            .map(|m| m.len())
            .unwrap_or(0);
        observe_snapshot_file_size(partition_id, self.node_id, installed_bytes);
        reap_superseded_snapshots(
            &self.snapshot_dir,
            &stored.path,
            &self.in_flight_snapshots,
            partition_id,
            self.node_id,
        );
        let previous = self
            .current_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .replace(stored);
        if let Some(previous) = previous {
            let _ = std::fs::remove_file(&previous.path);
        }
        // Publication + reap are complete: release the publication lock BEFORE
        // the adoption below enters the journal's checkpoint-permit domain.
        drop(publish_guard);

        if adopt {
            // Rebuild the engine actor's state from the captured snapshot IN
            // PLACE. The engine journal is in-memory under Raft (the Raft log is
            // the durable tier), so adopting the snapshot's engine state is the
            // install — but the journal's read-model exporter wiring (and
            // partition id / var store / spill tiers) MUST survive it, or an
            // owned partition catching up via a snapshot install stops
            // projecting and leaks its completed backlog. Adopt it through the
            // single adoption routine, which also checkpoints it as the
            // journal's recovery base BEFORE any later entry is accepted
            // (#1405). The captured engine state carries the snapshot's applied
            // record, so a crash before the pointer above is published still
            // boots from a journal that wins at the snapshot's index. Fail
            // closed: a replica whose journal cannot hold the base must not
            // keep applying.
            self.adopt_engine_snapshot(captured, meta)
                .await
                .map_err(|e| {
                    StorageIOError::write_snapshot(
                        Some(meta.signature()),
                        &std::io::Error::other(e.to_string()),
                    )
                })?;
        }

        {
            let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.last_applied = meta.last_log_id;
            inner.last_membership = meta.last_membership.clone();
        }
        Ok(())
    }
}

/// Filename prefix for the per-process, per-partition snapshot staging dirs a
/// volatile-log member anchors its snapshots under (see
/// [`PartitionStateMachine::temp_snapshot_dir`]). Shared with
/// [`sweep_orphaned_snapshot_dirs`] so creation and cleanup never drift.
const RAFT_SNAPSHOT_DIR_PREFIX: &str = "nanobpmn-raftsnap-";

/// Upper bound on how long [`RaftPartition::retire`] waits for in-flight
/// detached snapshot builds to drain before giving up on reclaiming the volatile
/// snapshot dir (#1473). A build normally finishes in well under a second (one
/// bounded serialize + fsync); 30 s bounds the wait against a wedged build
/// without blocking a rebuild forever — past it the dir is left for the next
/// boot's orphan sweep.
const RETIRE_DRAIN_BOUND: std::time::Duration = std::time::Duration::from_secs(30);

/// Whether `pid` names a live process. Linux-only signal via `/proc/<pid>`; on
/// other platforms (dev/test) we conservatively report "alive" so the sweep
/// never removes a dir it cannot prove is orphaned.
fn pid_is_alive(pid: u32) -> bool {
    if cfg!(target_os = "linux") {
        std::path::Path::new(&format!("/proc/{pid}")).exists()
    } else {
        true
    }
}

/// Remove orphaned per-process snapshot staging dirs left in the system temp dir
/// by dead nano processes. Each volatile-log (receiver / failover) member anchors
/// its snapshots under `nanobpmn-raftsnap-<pid>-p<part>-<ts>`; a member rebuild
/// (new ts) or a process restart (new pid) orphans the old dir, and an aborted
/// `InstallSnapshot` can leave a multi-GB partial inside it. Nothing else ever
/// reclaims them, so across restarts they can fill the disk (observed: 175+ GB on
/// a soak node, tripping the deploy disk preflight). Swept once at raft bootstrap:
/// a dir is removed only when its embedded pid is neither this process nor a live
/// one, so a co-located nano instance is never disturbed.
pub fn sweep_orphaned_snapshot_dirs() {
    let me = std::process::id();
    let Ok(entries) = std::fs::read_dir(std::env::temp_dir()) else {
        return;
    };
    for entry in entries.flatten() {
        if !entry.file_type().map(|t| t.is_dir()).unwrap_or(false) {
            continue;
        }
        let name = entry.file_name();
        let Some(pid) = name
            .to_str()
            .and_then(|n| n.strip_prefix(RAFT_SNAPSHOT_DIR_PREFIX))
            .and_then(|rest| rest.split('-').next())
            .and_then(|pid| pid.parse::<u32>().ok())
        else {
            continue;
        };
        if pid == me || pid_is_alive(pid) {
            continue;
        }
        let path = entry.path();
        tracing::info!(
            ?path,
            orphaned_pid = pid,
            "reclaiming orphaned raft snapshot staging dir"
        );
        let _ = std::fs::remove_dir_all(&path);
    }
}

/// A point-in-time snapshot builder: holds an [`EngineSnapshot`] and metadata
/// captured atomically (relative to `apply`) when the state-machine worker minted
/// it, so [`build_snapshot`](RaftSnapshotBuilder::build_snapshot) only has to
/// serialize an already-consistent state — no engine round-trip, no race with a
/// concurrent apply.
///
/// `captured` holds the streaming capture parts ([`Journal::streaming_snapshot_parts`]):
/// the control skeleton, the resident per-instance payloads, the cold routing
/// keys, and the store handles to resolve off-heap rows. `build_snapshot`
/// streams them to disk in bounded chunks, so the whole cold/spilled set is
/// never folded into one in-memory `EngineSnapshot` (#1337). The serialize is
/// **fail-closed**: if any cold/spilled row will not read, `build_snapshot`
/// returns a [`StorageError`] **before** publishing anything, so openraft treats
/// the build as failed and — critically — does **not** purge the covered log. An
/// incomplete snapshot is therefore never minted as the durable recovery point:
/// the log that can still regenerate the omitted state is preserved intact on
/// disk, so no data is lost. This is a hard fail-stop, not a graceful retry:
/// openraft propagates the `StorageError` as a `Fatal`, which drives this
/// partition's Raft core to [`ServerState::Shutdown`](openraft::ServerState::Shutdown)
/// (see [`is_shutdown`](RaftPartition::is_shutdown)). There is no auto-restart
/// in this crate, so the partition stops applying entries and every
/// instance/job routed here is stranded until the process is restarted — at
/// which point recovery replays the preserved log. The unreadable payload is
/// thus traded for availability loss, never data loss.
pub struct PartitionSnapshotBuilder {
    sm: Arc<PartitionStateMachine>,
    /// The streaming capture parts (#1337): a small control skeleton plus the
    /// resident per-instance payloads, the cold routing keys, and the store
    /// handles to resolve off-heap rows during the streaming serialize. `None`
    /// once taken by `build_snapshot`, or when the journal was fenced after an
    /// aborted Raft entry (#1405) so no capture was taken. Holding the parts — rather than one folded
    /// `EngineSnapshot` — is what lets `build_snapshot` stream the cold set
    /// straight from the spill store to disk without materializing it in RAM.
    captured: Option<(
        nano_server_storage::snapstream::StreamingCapture,
        Vec<nanobpmn_engine_core::Key>,
        nano_server_storage::snapstream::SpillStores,
    )>,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
    /// Test-only barrier waited on just before the build takes the snapshot
    /// publication lock (#1488 review regression): lets a test hold a build at
    /// "file written, publish not yet started" while a concurrent install
    /// publishes and returns, deterministically forcing the
    /// build-sweep-after-install-publish interleaving the snapshot publication
    /// lock protects against.
    #[cfg(test)]
    test_writer_barrier: Option<Arc<tokio::sync::Barrier>>,
    /// Test-only `(reached, release)` barrier pair (#1488 review regression):
    /// parks the build INSIDE the publication lock — after the pointer +
    /// cache publish, before the reap — so a test can prove a competing
    /// publisher cannot publish until this one releases the lock.
    #[cfg(test)]
    test_publish_barrier: Option<(Arc<tokio::sync::Barrier>, Arc<tokio::sync::Barrier>)>,
}

impl RaftSnapshotBuilder<RaftConfig> for PartitionSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<RaftConfig>, StorageError<NodeId>> {
        // Fail closed: refuse to publish a snapshot whose off-heap fold was
        // incomplete. openraft purges the log this snapshot subsumes as soon as a
        // `build_snapshot` succeeds, so sealing an incomplete capture here would
        // permanently discard the only copy of the omitted cold/spilled state.
        // Returning an error aborts the build before the durable pointer is
        // written and before the log is purged, so the covered log is preserved
        // and no data is lost. Note this is a hard fail-stop, not a retry:
        // openraft propagates this `StorageError` as a `Fatal`, shutting down this
        // partition's Raft core (`ServerState::Shutdown`, see `is_shutdown`). The
        // partition then stops applying entries and strands its instances/jobs
        // until the process restarts and replays the preserved log — availability
        // is sacrificed to guarantee the omitted state is never lost.
        let (capture, cold_keys, stores) = self.captured.take().ok_or_else(|| {
            StorageError::from_io_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                std::io::Error::new(
                    std::io::ErrorKind::InvalidData,
                    "refusing to build a Raft snapshot: no streaming capture (the \
                     journal is fenced after an aborted Raft entry, #1405, or the parts \
                     were already consumed by a second build_snapshot call); aborting \
                     before the covered log is purged",
                ),
            )
        })?;
        let snapshot_idx = self.sm.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let snapshot_id = if let Some(last) = self.last_applied {
            format!("{}-{}-{}", last.leader_id, last.index, snapshot_idx)
        } else {
            format!("--{snapshot_idx}")
        };

        let meta = SnapshotMeta {
            last_log_id: self.last_applied,
            last_membership: self.last_membership.clone(),
            snapshot_id,
        };

        // Stream the state capture straight to disk in bounded chunks (#1337)
        // rather than folding the whole cold/spilled set into one in-memory
        // `EngineSnapshot` and serializing it. The control skeleton + resident
        // payloads are already captured; each cold instance is read row-by-row
        // from the spill store and each spilled variable map resolved as it
        // serializes, so the peak transient is one chunk, never the whole
        // working set. Fail-closed: an unreadable cold/spilled row aborts the
        // write (a `StorageError`) before the durable pointer is written and
        // before the log is purged — exactly the old fail-closed fold semantics.
        //
        // Fix B: cap concurrent snapshot builds across this node's partitions. While
        // the resident SM is large (a returning owner / failover incumbent, see
        // `snapshot_recovery_engaged`) take the whole pool so the co-hosted reclaim
        // builds serialize onto the shared disk one at a time instead of piling four
        // simultaneous large fsyncs (~7x fsync amplification). Held across the
        // serialize + `sync_all` (+ the durable-pointer fsync) until this returns.
        let build_permits = if snapshot_recovery_engaged() {
            snapshot_build_concurrency()
        } else {
            1
        };
        let _build_permit = snapshot_build_sem().acquire_many(build_permits).await.ok();
        // Enter the engine lifecycle lock (#1473) AFTER the build semaphore (the
        // documented lock order) and hold it across the whole serialize +
        // publish: this build is a DETACHED task openraft never joins, so
        // without it a build could write into a snapshot dir its successor
        // reopened. A retired member is refused HERE, before any file I/O; and
        // retirement cannot complete while this guard is held.
        let _lifecycle = self.sm.enter_lifecycle().await.ok_or_else(|| {
            PartitionStateMachine::retired_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Write,
                "build a Raft snapshot",
            )
        })?;
        // Generation-stamped name (#1473): a predecessor build that outlives its
        // member can never collide with the successor's `snap-g<N+1>-…` names on
        // the reopened durable dir.
        let path = self
            .sm
            .snapshot_dir
            .join(format!("snap-g{}-{snapshot_idx}.bin", self.sm.generation));
        // Register the path as in flight BEFORE the file exists (#1488), so a
        // concurrent install's reap never deletes it mid-write / pre-publish.
        // The streaming writer stages the body in `path.with_extension("tmp")`
        // and renames it into place, so the `.tmp` staging file is registered
        // too — it matches the reaper's `snap-` prefix and is otherwise
        // deletable out from under the writer's reopen/rename.
        let _in_flight = InFlightSnapshot::register(&self.sm.in_flight_snapshots, &path);
        let _in_flight_tmp =
            InFlightSnapshot::register(&self.sm.in_flight_snapshots, &path.with_extension("tmp"));
        let covered = self.last_applied.map(|l| l.index).unwrap_or(0);
        let serialize_start = std::time::Instant::now();
        // The streaming serialize blocks on file I/O and the spill-store reads;
        // run it on a blocking task (not the async executor / engine actor). The
        // store handles were captured on the engine thread, so this is purely
        // synchronous — no nested engine round-trip.
        let write_path = path.clone();
        let write_result = tokio::task::spawn_blocking(move || {
            let header = nano_server_storage::seglog::snapshot_envelope_header_line(
                write_path
                    .parent()
                    .unwrap_or_else(|| std::path::Path::new(".")),
            )?;
            nano_server_storage::snapstream::write_streaming_snapshot_from_stores(
                &write_path,
                &header,
                covered,
                // Not a seglog journal base: no rebase-floor identity (#1405).
                None,
                capture,
                &cold_keys,
                stores,
                // Raft snapshots are self-contained (never lean/control-only):
                // resolve every spilled placeholder so a follower installs every
                // variable with no dependency on any store.
                false,
            )
        })
        .await
        .map_err(|e| {
            StorageIOError::write_snapshot(Some(meta.signature()), &std::io::Error::other(e))
        })?;
        // Fail closed: an unreadable off-heap row aborts the build before the
        // durable pointer is written and before openraft purges the covered log.
        write_result.map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        let file = std::fs::File::open(&path)
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        let serialize_dur = serialize_start.elapsed();
        let snapshot_bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
        // Durable enough to serve to a follower even across a crash: the log is
        // still the authoritative tier, but a torn snapshot must never be shipped.
        let fsync_start = std::time::Instant::now();
        file.sync_all()
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        // Attribute the returning-owner recovery notch to snapshot-build IO: a
        // large resident SM makes this serialize + sync_all stall the shared
        // Raft-log fsync path (see nanobpm_raft_snapshot_* metrics).
        crate::metrics::observe_snapshot_build(
            serialize_dur,
            fsync_start.elapsed(),
            snapshot_bytes,
        );

        // Record the durable pointer to this snapshot BEFORE anything unlinks the
        // one it replaces AND before build_snapshot returns: openraft may purge
        // the log this snapshot subsumes as soon as it returns, so a restartable
        // recovery point must already be on disk (see [`PersistedSnapshotPtr`]).
        // The publish + reap below run under the partition's snapshot
        // publication lock (#1488 review): a concurrent install's publish can
        // then never interleave with this sweep, so the sweep can never delete
        // a file the install just made the durable recovery point.
        //
        // Test-only barrier (#1488 review regression): park the build just
        // BEFORE it takes the publication lock, so a test can interleave a
        // full install publish+return and then release this build into its
        // publish+sweep — the exact "build sweeps after the install's guard
        // dropped" interleaving the publication lock protects against.
        #[cfg(test)]
        if let Some(barrier) = self.test_writer_barrier.take() {
            barrier.wait().await;
        }
        let _publish = self.sm.engine.snapshot_publish_lock().await;
        // Never regress the published snapshot (#1488 review): a build
        // captured at index N that reaches publication after a concurrent
        // install already published M > N must not replace the pointer/cache
        // (OpenRaft may already purge through M). Discard this stale build and
        // hand back the newer published snapshot instead.
        let newer = published_snapshot_supersedes(&self.sm.current_snapshot, &meta);
        if let Some(newer) = newer {
            let _ = std::fs::remove_file(&path);
            let tokio_file = tokio::fs::File::open(&newer.path)
                .await
                .map_err(|e| StorageIOError::read_snapshot(Some(newer.meta.signature()), &e))?;
            return Ok(Snapshot {
                meta: newer.meta,
                snapshot: Box::new(SnapshotFile {
                    file: tokio_file,
                    path: newer.path,
                    in_flight: None,
                }),
            });
        }
        let stored = StoredSnapshot {
            meta: meta.clone(),
            path: path.clone(),
        };
        write_snapshot_ptr(&self.sm.snapshot_dir, &stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        // Publish as the current snapshot and unlink the file it replaces.
        let previous = self
            .sm
            .current_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .replace(stored);
        if let Some(previous) = previous.filter(|p| p.path != path) {
            let _ = std::fs::remove_file(&previous.path);
        }
        #[cfg(test)]
        if let Some((reached, release)) = self.test_publish_barrier.take() {
            reached.wait().await;
            release.wait().await;
        }
        // #1488: publish the per-partition/per-replica snapshot size (+ the
        // oversize alert) and reap every OTHER superseded artifact still in the
        // dir — an orphaned older-generation or un-tagged file must not
        // accumulate until the next process restart sweeps it.
        let partition_id = self.sm.partition_id();
        observe_snapshot_file_size(partition_id, self.sm.node_id, snapshot_bytes);
        reap_superseded_snapshots(
            &self.sm.snapshot_dir,
            &path,
            &self.sm.in_flight_snapshots,
            partition_id,
            self.sm.node_id,
        );

        let tokio_file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(SnapshotFile {
                file: tokio_file,
                path,
                in_flight: None,
            }),
        })
    }
}

impl RaftStateMachine<RaftConfig> for Arc<PartitionStateMachine> {
    type SnapshotBuilder = PartitionSnapshotBuilder;

    async fn applied_state(
        &mut self,
    ) -> Result<(Option<LogId<NodeId>>, StoredMembership<NodeId, BasicNode>), StorageError<NodeId>>
    {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ReplicatedResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<RaftConfig>> + Send,
    {
        // Engine lifecycle lock (#1473): hold it SHARED for the whole batch, so a
        // retirement (exclusive) can neither land mid-batch nor be followed by
        // an apply that resumes from a durability barrier and submits entries
        // to an engine a successor now owns. A retired member is refused.
        let _lifecycle = self.enter_lifecycle().await.ok_or_else(|| {
            PartitionStateMachine::retired_error(
                ErrorSubject::StateMachine,
                ErrorVerb::Write,
                "apply a Raft entry",
            )
        })?;
        let mut responses = Vec::new();
        // Each Normal entry is a BATCH of commands. We apply the whole batch in a
        // single engine-actor round-trip (the actor runs them in submission order),
        // collecting each command's events + durable-commit barrier, then await all
        // the barriers together — so the journal's group-commit writer coalesces the
        // batch into one write+fsync instead of one fsync per command. We never hold
        // the std::Mutex across an engine `.await`.
        for entry in entries {
            let log_id = entry.log_id;
            match entry.payload {
                EntryPayload::Blank => {
                    self.inner
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .last_applied = Some(log_id);
                    responses.push(ReplicatedResponse::default());
                }
                EntryPayload::Normal(batch) => {
                    // Entry-level atomic apply (#1405): apply every command of the
                    // batch against the LIVE journal in ONE actor hop, BUFFERING
                    // the events, then journal the WHOLE entry — every command's
                    // events plus a `RaftEntryApplied` record naming this log id —
                    // as ONE atomic journal frame with a single durability barrier.
                    // The reader drops a torn frame whole, so a crash can never
                    // keep part of an entry, and the journal knows atomically which
                    // entry it last applied (bootstrap seeds `last_applied` from it,
                    // see `seed_last_applied_from_journal`).
                    //
                    // Per-command error classification
                    // (`EngineError::is_environmental`, #1388):
                    //
                    // * DETERMINISTIC rejection — a pure function of `(command,
                    //   replicated state)`, identical on every replica. It is
                    //   recorded against its command and the batch CONTINUES.
                    //
                    // * ENVIRONMENTAL failure — an off-heap row THIS replica cannot
                    //   read. The entry FAILS CLOSED: the buffer is discarded (the
                    //   journal never sees any of the entry), `last_applied` does not
                    //   move, and a `StorageError` makes openraft shut this replica's
                    //   Raft core down. The live engine may hold the entry's partial
                    //   effects, but they were never durable: recovery rebuilds from
                    //   the journal, so nothing needs undoing and no non-idempotent
                    //   prefix command can execute twice.
                    //
                    // The command `kind` is captured alongside each outcome so the
                    // error-mapping phase below can apply the command-specific HTTP
                    // status contract (`engine_error_status_for`).
                    type ApplyOutcome = Result<Arc<Vec<Event>>, nanobpmn_engine_core::EngineError>;
                    let raft_state = {
                        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                        journaled_raft_state(log_id, &inner.last_membership)
                    }
                    .map_err(|e| {
                        StorageError::from_io_error(ErrorSubject::StateMachine, ErrorVerb::Write, e)
                    })?;
                    type EntryOutcomes = Vec<(&'static str, ApplyOutcome)>;
                    let applied: std::io::Result<(EntryOutcomes, crate::journal::Commit)> = self
                        .engine
                        .with(move |journal| {
                            journal.begin_raft_entry();
                            let mut outcomes = Vec::with_capacity(batch.items.len());
                            for ReplicatedCommand { command, now } in batch.items {
                                // Per-command actor profiling (off unless
                                // NANOBPM_CMD_PROFILE): time + allocated-byte
                                // delta by command kind, on the engine thread.
                                let timer = crate::cmd_profile::start();
                                let kind = command.kind();
                                let outcome = journal.apply_command_at(command, now);
                                crate::cmd_profile::finish(timer, kind);
                                match outcome {
                                    Err(e) if e.is_environmental() => {
                                        journal.abort_raft_entry();
                                        return Err(std::io::Error::other(format!(
                                            "environmental failure applying committed \
                                             batch: {e}"
                                        )));
                                    }
                                    outcome => outcomes.push((kind, outcome.map(|(ev, _)| ev))),
                                }
                            }
                            let (_, commit) = journal
                                .commit_raft_entry(log_id.index, raft_state)
                                .inspect_err(|_| journal.abort_raft_entry())?;
                            Ok((outcomes, commit))
                        })
                        .await;
                    let (outcomes, commit) = match applied {
                        Ok(ok) => ok,
                        Err(e) => {
                            tracing::error!(
                                error = %e,
                                "raft apply: failing closed on a committed entry \
                                 (nothing journaled, last_applied NOT advanced) — \
                                 recover this replica from its journal (#1405)"
                            );
                            return Err(StorageError::from_io_error(
                                ErrorSubject::StateMachine,
                                ErrorVerb::Write,
                                e,
                            ));
                        }
                    };
                    // The entry's single durability barrier.
                    commit.wait().await;

                    // Build the per-command responses.
                    let mut items = Vec::with_capacity(outcomes.len());
                    // A follower replica has no read-model exporter to drive
                    // hot-state eviction, so reclaim each instance's shell (+ its
                    // completed job records) the moment it reaches a terminal
                    // state. But only while this member is NOT the partition
                    // leader: the instant it wins leadership (elected or promoted
                    // after a failover) it serves reads/status from this engine,
                    // so it must keep terminal shells resident — exactly the
                    // ADR-0012 reason an owned leader defers to its exporter.
                    let evict_terminal =
                        self.evict_eligible && self.leader.load(Ordering::Relaxed) != self.node_id;
                    let mut terminal: Vec<nanobpmn_engine_core::Key> = Vec::new();
                    for outcome in outcomes {
                        match outcome {
                            (_, Ok(events)) => {
                                if evict_terminal {
                                    terminal.extend(
                                        events.iter().filter_map(|e| e.terminal_instance_key()),
                                    );
                                }
                                items.push(ReplicatedItem {
                                    events: events.to_vec(),
                                    error: None,
                                });
                            }
                            // A rejected command produced no events; the log entry is
                            // still consumed so every replica stays in lockstep. The
                            // leader surfaces the mapped rejection to its client,
                            // keyed on the command kind so each endpoint keeps its
                            // HTTP status contract across both deployment modes. #1308.
                            (kind, Err(e)) => {
                                items.push(ReplicatedItem {
                                    events: Vec::new(),
                                    error: Some(engine_error_status_for(kind, &e)),
                                });
                            }
                        }
                    }
                    if !terminal.is_empty() {
                        self.engine.spawn_job(move |journal| {
                            journal.evict_instances(&terminal);
                        });
                    }
                    self.inner
                        .lock()
                        .unwrap_or_else(|e| e.into_inner())
                        .last_applied = Some(log_id);
                    responses.push(ReplicatedResponse { items });
                }
                EntryPayload::Membership(mem) => {
                    // Journal the applied-entry record carrying the NEW
                    // membership (#1405) so a snapshot-less bootstrap that seeds
                    // `last_applied` from the journal also recovers the
                    // membership the skipped log prefix established.
                    let membership = StoredMembership::new(Some(log_id), mem);
                    let raft_state = journaled_raft_state(log_id, &membership).map_err(|e| {
                        StorageError::from_io_error(ErrorSubject::StateMachine, ErrorVerb::Write, e)
                    })?;
                    let commit = self
                        .engine
                        .with(move |journal| {
                            journal.begin_raft_entry();
                            journal
                                .commit_raft_entry(log_id.index, raft_state)
                                .map(|(_, commit)| commit)
                                .inspect_err(|_| journal.abort_raft_entry())
                        })
                        .await
                        .map_err(|e| {
                            StorageError::from_io_error(
                                ErrorSubject::StateMachine,
                                ErrorVerb::Write,
                                e,
                            )
                        })?;
                    commit.wait().await;
                    let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
                    inner.last_applied = Some(log_id);
                    inner.last_membership = membership;
                    responses.push(ReplicatedResponse::default());
                }
            }
        }
        Ok(responses)
    }

    async fn get_snapshot_builder(&mut self) -> Self::SnapshotBuilder {
        // Capture the engine's materialized state and the applied metadata as a
        // consistent pair. This runs on the state-machine worker, which drives
        // `apply` and snapshot building sequentially, so no command is applied
        // between the engine read and the `last_applied`/membership read — the
        // captured state corresponds exactly to `last_applied`.
        //
        // Capture the streaming parts (#1337): the control skeleton + the
        // resident per-instance payloads + the cold routing keys + the store
        // handles to resolve off-heap rows. This never folds the whole
        // cold/spilled set into one in-memory `EngineSnapshot` — that
        // materialization is the #1337 defect. The capture only reads live state
        // + the cold index; the cold / spilled rows are read from the store
        // during `build_snapshot`'s streaming serialize, which fails closed (a
        // `StorageError`, so the covered log is never purged) if any row will
        // not read.
        // Capture under the engine lifecycle lock (#1473). A retired member (a
        // queued `BuildSnapshot` that runs after retirement) captures NOTHING,
        // so its `build_snapshot` fails closed before any file I/O.
        let captured = match self.enter_lifecycle().await {
            Some(_lifecycle) => {
                self.engine
                    .with(|journal| journal.streaming_snapshot_parts())
                    .await
            }
            None => None,
        };
        let (last_applied, last_membership) = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            (inner.last_applied, inner.last_membership.clone())
        };
        PartitionSnapshotBuilder {
            sm: self.clone(),
            captured,
            last_applied,
            last_membership,
            #[cfg(test)]
            test_writer_barrier: None,
            #[cfg(test)]
            test_publish_barrier: None,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotFile>, StorageError<NodeId>> {
        // Engine lifecycle lock (#1473): held SHARED across the sweep AND the
        // file open below — not just a one-shot probe. A predecessor receive
        // that passed a bare `is_retired()` check could still be parked in the
        // `truncate(true)` open while retirement constructs a durable successor
        // on the SAME snapshot dir; both members derive their receive path from
        // the same partition id and process id (`incoming-<pid>-<seq>.tmp`) and
        // each member's `recv_idx` restarts at 0, so the predecessor's delayed
        // open would truncate the successor's just-received incoming snapshot.
        // Holding the epoch-checked guard means retirement cannot complete
        // until this open has finished, and a receive that starts after
        // retirement is refused before it touches the dir.
        let _lifecycle = self.enter_lifecycle().await.ok_or_else(|| {
            PartitionStateMachine::retired_error(
                ErrorSubject::Snapshot(None),
                ErrorVerb::Read,
                "receive a Raft snapshot",
            )
        })?;
        // Reclaim any orphaned partials from previously-aborted installs in this
        // dir before starting a fresh receive. openraft never resumes a prior
        // `begin_receiving_snapshot` file, so any leftover `incoming-*.tmp` is dead
        // weight: under a catch-up-timeout retry loop (ADR 0019 snapshot churn)
        // each aborted InstallSnapshot would otherwise leave a multi-GB partial
        // behind, and they accumulate until the disk fills. Sweeping here bounds
        // the in-flight partials for this partition to one.
        if let Ok(entries) = std::fs::read_dir(&self.snapshot_dir) {
            for entry in entries.flatten() {
                if entry
                    .file_name()
                    .to_str()
                    .is_some_and(|n| n.starts_with("incoming-"))
                {
                    let _ = std::fs::remove_file(entry.path());
                }
            }
        }
        // A fresh, empty on-disk file that openraft streams the incoming snapshot
        // chunks into (AsyncWrite + AsyncSeek), so the receiving side never buffers
        // the whole snapshot in RAM either.
        let seq = self.recv_idx.fetch_add(1, Ordering::Relaxed);
        let path = self
            .snapshot_dir
            .join(format!("incoming-{}-{seq}.tmp", std::process::id()));
        // Register the receive in flight BEFORE the file is created (#1488
        // review): a concurrent build's publish+reap sweeps every unmatched
        // artifact, so an unregistered receive would be deleted mid-stream.
        let in_flight = InFlightSnapshot::register(&self.in_flight_snapshots, &path);
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .await
            .map_err(|e| StorageIOError::write_snapshot(None, &e))?;
        Ok(Box::new(SnapshotFile {
            file,
            path,
            in_flight: Some(in_flight),
        }))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError<NodeId>> {
        self.install_snapshot_inner(meta, snapshot, true).await
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RaftConfig>>, StorageError<NodeId>> {
        // Copy the small (meta, path) pair out from under the lock so the file
        // open can `.await` without holding the std mutex.
        let entry = {
            let guard = self
                .current_snapshot
                .lock()
                .unwrap_or_else(|e| e.into_inner());
            guard.as_ref().map(|s| (s.meta.clone(), s.path.clone()))
        };
        match entry {
            Some((meta, path)) => {
                let file = tokio::fs::File::open(&path)
                    .await
                    .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(SnapshotFile {
                        file,
                        path,
                        in_flight: None,
                    }),
                }))
            }
            None => Ok(None),
        }
    }
}

fn raft_env_u64(key: &str, default: u64) -> u64 {
    std::env::var(key)
        .ok()
        .and_then(|v| v.parse().ok())
        .unwrap_or(default)
}

/// Max concurrent Raft snapshot builds across all of a node's partitions
/// (`NANOBPMN_RAFT_SNAPSHOT_BUILD_CONCURRENCY`, default 2, floored at 1).
fn snapshot_build_concurrency() -> u32 {
    raft_env_u64("NANOBPMN_RAFT_SNAPSHOT_BUILD_CONCURRENCY", 2).clamp(1, 4096) as u32
}

/// Serialized-snapshot byte threshold above which a node is treated as being in the
/// "large state machine" window — a returning owner draining a deep reclaim backlog,
/// or a failover incumbent — that makes snapshot builds expensive
/// (`NANOBPMN_RAFT_RECOVERY_SNAPSHOT_BYTES`, default 32 MiB).
fn snapshot_recovery_bytes_threshold() -> u64 {
    raft_env_u64("NANOBPMN_RAFT_RECOVERY_SNAPSHOT_BYTES", 32 * 1024 * 1024)
}

/// Snapshot-FILE byte ceiling for the oversize alert (#1488): when a partition's
/// current snapshot `.bin` on this replica exceeds this, the publish helper logs
/// a WARN naming the partition, the replica, and the size. This is the "cap/alert"
/// half of #100's residual scope — the #100 blowup wrote 8.4 GB follower
/// snapshots against a 435 KB leader snapshot of the same partition, and nothing
/// flagged it until the disk filled. The default (1 GiB) is ~2000x the healthy
/// per-partition snapshot size observed in that soak, so it fires only on a real
/// blowup, never on ordinary growth. `0` disables the WARN (the
/// `nanobpm_raft_snapshot_file_bytes` gauge is still published for a
/// Prometheus-side threshold alert).
/// (`NANOBPMN_RAFT_SNAPSHOT_ALERT_BYTES`, default 1 GiB.)
fn snapshot_alert_bytes_threshold() -> u64 {
    raft_env_u64("NANOBPMN_RAFT_SNAPSHOT_ALERT_BYTES", 1024 * 1024 * 1024)
}

/// Publish one partition's current snapshot-file size on this replica: the
/// per-partition/per-replica `nanobpm_raft_snapshot_file_bytes` gauge plus the
/// oversize WARN alert (#1488). Called on every local snapshot build and every
/// follower snapshot install, so both the leader's and the followers' snapshot
/// files carry a size series — an oversized snapshot on ONE replica of ONE
/// partition is then visible per-replica instead of hiding inside the
/// process-wide `raft_snapshot_bytes` last-write gauge.
fn observe_snapshot_file_size(partition_id: u64, node_id: NodeId, bytes: u64) {
    crate::metrics::set_raft_snapshot_file_bytes(partition_id, node_id, bytes);
    let threshold = snapshot_alert_bytes_threshold();
    if threshold > 0 && bytes > threshold {
        tracing::warn!(
            partition = partition_id,
            node = node_id,
            snapshot_bytes = bytes,
            threshold_bytes = threshold,
            "Raft snapshot file exceeds the alert threshold \
             (NANOBPMN_RAFT_SNAPSHOT_ALERT_BYTES): an oversized snapshot on one \
             replica of one partition is the #100 blowup signature — check for \
             follower snapshot divergence before the disk fills"
        );
    }
}

/// The single non-regression rule for snapshot publication (#1488 review):
/// returns the currently published snapshot iff it is strictly NEWER than
/// `candidate`, in which case publishing `candidate` would move the durable
/// recovery point backwards. Must be called under the snapshot publication
/// lock so the check and the publish are atomic.
fn published_snapshot_supersedes(
    current: &Mutex<Option<StoredSnapshot>>,
    candidate: &SnapshotMeta<NodeId, BasicNode>,
) -> Option<StoredSnapshot> {
    current
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .as_ref()
        .filter(|cur| cur.meta.last_log_id > candidate.last_log_id)
        .cloned()
}

/// Reap every superseded snapshot artifact in `snapshot_dir` that is not the
/// CURRENT snapshot `keep` (#1488). `build_snapshot` and `install_snapshot`
/// unlink only the one file they directly replace, so any older artifact — a
/// pre-#1473 un-tagged `snap-<idx>.bin`, a same-generation file orphaned by a
/// crash between the file write and the durable-pointer write, or a
/// predecessor-generation file a late build/install left behind — accumulated
/// on the replica until a process restart swept it. Under a catch-up retry
/// loop those orphans are multi-GB each (the #100 follower blowup), so they are
/// reaped here, the moment a newer snapshot becomes current, rather than at
/// the next boot. Only recognized snapshot artifacts (`snap-*.bin` /
/// `snap-installed-*.bin`) are touched; the durable pointer and foreign files
/// are left alone, as is every path still registered in `in_flight` (a
/// concurrent build/install that has not yet published it). Best-effort: a
/// removal failure is logged, never fatal.
fn reap_superseded_snapshots(
    snapshot_dir: &Path,
    keep: &Path,
    in_flight: &Mutex<std::collections::HashSet<PathBuf>>,
    partition_id: u64,
    node_id: NodeId,
) {
    let entries = match std::fs::read_dir(snapshot_dir) {
        Ok(entries) => entries,
        Err(e) => {
            tracing::warn!(
                partition = partition_id,
                node = node_id,
                error = %e,
                "could not list the snapshot dir to reap superseded snapshots"
            );
            return;
        }
    };
    for entry in entries.flatten() {
        let path = entry.path();
        if path == keep {
            continue;
        }
        let ours = path
            .file_name()
            .and_then(|n| n.to_str())
            .map(|n| n.starts_with("snap-"))
            .unwrap_or(false);
        if !ours {
            continue;
        }
        // Registration precedes file creation, so a listed file a concurrent
        // build/install is still producing is always visible here.
        if in_flight
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .contains(&path)
        {
            continue;
        }
        match std::fs::remove_file(&path) {
            Ok(()) => {
                tracing::info!(
                    partition = partition_id,
                    node = node_id,
                    reaped = ?path,
                    "reaped a superseded Raft snapshot file"
                );
            }
            Err(e) => {
                tracing::warn!(
                    partition = partition_id,
                    node = node_id,
                    reaped = ?path,
                    error = %e,
                    "could not reap a superseded Raft snapshot file"
                );
            }
        }
    }
}

/// RAII registration of a snapshot file a build/install is still producing
/// (#1488): while alive, [`reap_superseded_snapshots`] skips the path. Dropped
/// on every exit path (success or error), so a failed build never pins a path.
struct InFlightSnapshot {
    set: Arc<Mutex<std::collections::HashSet<PathBuf>>>,
    path: PathBuf,
}

impl InFlightSnapshot {
    fn register(set: &Arc<Mutex<std::collections::HashSet<PathBuf>>>, path: &Path) -> Self {
        set.lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(path.to_path_buf());
        Self {
            set: Arc::clone(set),
            path: path.to_path_buf(),
        }
    }
}

impl Drop for InFlightSnapshot {
    fn drop(&mut self) {
        self.set
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .remove(&self.path);
    }
}

/// Whether this node's resident state machine is currently large enough that its
/// snapshot builds are expensive (the returning-owner recovery window).
///
/// Keyed on the size of the last-built snapshot rather than a leadership signal:
/// `recovery_fsync_load_active` clears the instant a returning owner reclaims
/// leadership of its partitions, but the expensive build storm runs for the whole
/// time it then spends applying the deep reclaim backlog. The SM size directly
/// tracks that cost and self-releases as the backlog drains, so both the build
/// concurrency limiter (Fix B) and the cadence stretch (Fix C) engage exactly while
/// builds are large and disengage once the SM is back to its lean steady-state size.
pub fn snapshot_recovery_engaged() -> bool {
    crate::metrics::last_snapshot_bytes() as u64 >= snapshot_recovery_bytes_threshold()
}

/// Process-global limiter on concurrent snapshot builds (Fix B for the
/// returning-owner recovery notch).
///
/// A returning owner reclaims all its co-hosted partitions (e.g. 2,5,8,11) at once;
/// they cross their `LogsSinceLast` threshold together and call
/// [`build_snapshot`](PartitionStateMachine::build_snapshot) at nearly the same
/// instant. Each build does a blocking `serde_json` serialize + `sync_all` of a
/// large (~130 MB) state machine, so four fire simultaneously onto the one shared
/// disk and the fsync latency amplifies ~7x (≈96 ms un-contended → ≈711 ms under
/// 4-way contention), stalling the shared Raft-log fsync path and the
/// completion-paced admission servo. This semaphore caps how many build at once;
/// while the SM is large (see [`snapshot_recovery_engaged`]) a build takes the
/// *whole* pool (exclusive) so the co-hosted reclaim builds run strictly one at a
/// time.
fn snapshot_build_sem() -> &'static tokio::sync::Semaphore {
    static SNAPSHOT_BUILD_SEM: std::sync::OnceLock<tokio::sync::Semaphore> =
        std::sync::OnceLock::new();
    SNAPSHOT_BUILD_SEM.get_or_init(new_snapshot_build_sem)
}

/// Constructs the snapshot-build semaphore, sized from
/// [`snapshot_build_concurrency`] — the one place its capacity is derived.
fn new_snapshot_build_sem() -> tokio::sync::Semaphore {
    tokio::sync::Semaphore::new(snapshot_build_concurrency() as usize)
}

/// The per-partition snapshot cadence, in applied log entries, with a bounded
/// deterministic jitter so the partition replicas a single node hosts do not all
/// cross their snapshot threshold in the same instant.
///
/// Each partition's engine runs on its own single-threaded actor, and building a
/// snapshot briefly blocks that actor on an `O(working set)` `state.clone()` (the
/// serialize itself already runs off the actor, in `build_snapshot`). With a
/// uniform `LogsSinceLast(N)` every co-hosted partition reaches `N` at nearly the
/// same wall-clock time under steady load, so all of their engine actors stall
/// their creates/completes at once and aggregate throughput drops to a sharp
/// notch. Spreading the threshold by a per-partition-deterministic offset
/// staggers those clones so at most one or two partitions pause at a time — the
/// notch flattens into ripple.
///
/// The jitter is a percentage of the base (`NANOBPMN_RAFT_SNAPSHOT_JITTER_PCT`,
/// default 25, capped at 90; `0` disables it for an exact base, which keeps tests
/// that pin a small `NANOBPMN_RAFT_SNAPSHOT_LOGS` deterministic). It is centered
/// on the base, so the average snapshot frequency — and thus the memory/IO vs
/// log-length trade-off — is unchanged.
fn snapshot_logs_for_partition(partition_id: u64) -> u64 {
    let base = raft_env_u64("NANOBPMN_RAFT_SNAPSHOT_LOGS", 5000).max(1);
    let pct = raft_env_u64("NANOBPMN_RAFT_SNAPSHOT_JITTER_PCT", 25).min(90);
    jitter_snapshot_logs(base, pct, partition_id)
}

/// The pure, env-free core of [`snapshot_logs_for_partition`]: offset `base` by a
/// bounded, per-partition-deterministic amount within `± base * pct%`, centered on
/// `base`. `pct == 0` returns `base` unchanged. Split out so the jitter's
/// properties (bounded, centered, deterministic, well-spread) are unit-testable
/// without touching process-wide env.
fn jitter_snapshot_logs(base: u64, pct: u64, partition_id: u64) -> u64 {
    let base = base.max(1);
    let pct = pct.min(90);
    if pct == 0 {
        return base;
    }
    // ± this many entries around the base.
    let range = (base.saturating_mul(pct) / 100).max(1);
    // A Knuth multiplicative hash spreads consecutive partition ids evenly across
    // the whole [-range, +range] window, so neighbouring partitions (which a node
    // hosts as a contiguous block) land far apart rather than adjacent. The offset
    // arithmetic is done in i128 so it stays exact across the full u64 input
    // domain (a `base` near u64::MAX would overflow i64), then clamped back into
    // [1, u64::MAX].
    let span = range.saturating_mul(2).saturating_add(1);
    let hashed = partition_id.wrapping_mul(0x9E37_79B9_7F4A_7C15);
    let offset = (hashed % span) as i128 - range as i128;
    (base as i128 + offset).clamp(1, u64::MAX as i128) as u64
}

/// The shared openraft tuning for a nanobpmn partition group: a brisk cadence so
/// elections settle quickly. All three timings are env-overridable for tuning
/// (read once at bootstrap, never in the hot path) — on a heavily contended box a
/// calmer cadence can avoid heartbeat-miss election churn, but the brisk defaults
/// are what the failover tests and the A/B benchmark are validated against. The
/// snapshot cadence is jittered per partition (see
/// [`snapshot_logs_for_partition`]) so co-hosted partitions do not snapshot in
/// lockstep.
fn raft_config(partition_id: u64) -> Config {
    Config {
        heartbeat_interval: raft_env_u64("NANOBPMN_RAFT_HEARTBEAT_MS", 250),
        election_timeout_min: raft_env_u64("NANOBPMN_RAFT_ELECTION_MIN_MS", 500),
        election_timeout_max: raft_env_u64("NANOBPMN_RAFT_ELECTION_MAX_MS", 1000),
        // Snapshot every N applied log entries to compact the log (openraft
        // default 5000). Env-tunable so a deployment can trade snapshot frequency
        // (memory/IO) against log length, and so tests can force the snapshot
        // build/install path with a small threshold.
        snapshot_policy: openraft::SnapshotPolicy::LogsSinceLast(snapshot_logs_for_partition(
            partition_id,
        )),
        // Entries retained below the snapshot point for followers to catch up via
        // log replication instead of a full snapshot install. openraft's default
        // is 1000; under large variable payloads a single batched entry can be
        // >1 MB, so 1000 retained entries pin >1 GB per partition *even right
        // after a snapshot*. Env-tunable (`NANOBPMN_RAFT_KEEP_LOGS`) so a
        // large-payload deployment can shrink this floor — a lagging follower then
        // catches up from the (lean) snapshot, which for big payloads is cheaper
        // than shipping the retained log tail.
        max_in_snapshot_log_to_keep: raft_env_u64("NANOBPMN_RAFT_KEEP_LOGS", 1000),
        // Retention accelerator for the reclaim hand-off (ADR 0019): retain up to
        // this many extra already-snapshotted entries when a replication target is
        // behind, so a returning owner (added as a learner on rejoin) catches up by
        // STREAMING the retained tail instead of installing a full state-machine
        // snapshot — which under sustained load cannot finish inside the hand-off
        // catch-up window, so the transfer would otherwise only complete once load
        // eased. Bounded, so a stuck/dead target cannot pin the log without bound.
        // `0` disables it (pure `max_in_snapshot_log_to_keep` purging). Sized to
        // cover a realistic rejoin gap (downtime × per-partition write rate); under
        // large variable payloads a deployment should shrink it (each retained
        // entry can be ~1 MB), trading a snapshot install for retained-log memory.
        // (`NANOBPMN_RAFT_LAGGING_RETAIN`, default 400_000.)
        max_extra_log_to_keep_for_lagging: raft_env_u64("NANOBPMN_RAFT_LAGGING_RETAIN", 400_000),
        // Cap on entries coalesced into one AppendEntries RPC. openraft's default
        // is 300; combined with large (50 KB-variable) batched entries a single
        // catch-up RPC would carry hundreds of MB and blow the ~250 ms
        // AppendEntries timeout, so a lagging follower can never catch up and
        // replication collapses. Bounding entries-per-RPC (with the byte-bounded
        // entries from the propose batcher) keeps each AppendEntries shippable
        // within the timeout. Env-tunable (`NANOBPMN_RAFT_MAX_PAYLOAD_ENTRIES`).
        max_payload_entries: raft_env_u64("NANOBPMN_RAFT_MAX_PAYLOAD_ENTRIES", 16),
        // PER-CHUNK budget for an InstallSnapshot segment RPC. openraft's default
        // is a mere 200 ms, and because `send_snapshot_timeout` is 0 that same
        // value also bounds the FINAL segment — whose RPC only returns after the
        // receiver deserializes and applies the ENTIRE snapshot body
        // (PartitionStateMachine install_snapshot does a full serde_json read of
        // the resident engine state: every active process instance). For a large
        // state machine (millions of instances after a long-downtime rejoin under
        // load) that install takes seconds to tens of seconds, so the 200 ms
        // deadline elapses and openraft aborts + restarts the whole snapshot
        // forever (`InstallSnapshot RPC timed out: deadline has elapsed`,
        // request_id Snapshot(N) climbing) — the transfer never lands and a
        // returning owner can never catch up via snapshot install under load.
        //
        // We DON'T fix this with a bigger fixed guess (that rots as state grows).
        // The vendored snapshot transport derives the FINAL segment's deadline
        // from the snapshot SIZE: it budgets `ceil(snapshot_bytes / chunk_size)`
        // of THIS value, so the whole-install deadline auto-scales linearly with
        // the snapshot. This knob is therefore the per-chunk unit (transfer +
        // apply of one `snapshot_max_chunk_size` chunk); pick it generously (the
        // final install is slower per byte than raw transfer). Env-tunable.
        // (`NANOBPMN_RAFT_INSTALL_SNAPSHOT_TIMEOUT_MS`, 2s per chunk.)
        install_snapshot_timeout: raft_env_u64("NANOBPMN_RAFT_INSTALL_SNAPSHOT_TIMEOUT_MS", 2_000),
        // Per-RPC AppendEntries timeout, DECOUPLED from `heartbeat_interval`.
        // Upstream openraft times each AppendEntries out at one heartbeat (250ms
        // here), so a follower that is alive but momentarily busy — e.g. node18
        // just after it rejoins, simultaneously leading its own 4 partitions,
        // following the other 8, and draining a large catch-up — cannot ack in
        // time, and EVERY AppendEntries is aborted + re-sent. That retry storm
        // (thousands/sec, seen as `timeout after 250ms when AppendEntries 0->2`)
        // burns leader CPU and makes cluster throughput oscillate long after the
        // node is otherwise healthy. Keep fast heartbeats (quick failover) but
        // give each RPC a generous deadline so a loaded follower applies backlog
        // instead of thrashing. (`NANOBPMN_RAFT_APPEND_ENTRIES_TIMEOUT_MS`, 1s.)
        append_entries_timeout: raft_env_u64("NANOBPMN_RAFT_APPEND_ENTRIES_TIMEOUT_MS", 1_000),
        ..Default::default()
    }
}

/// Per-partition live-log byte ceiling that drives a byte-based snapshot: when a
/// partition's non-purged log exceeds this, the compaction governor triggers a
/// snapshot regardless of the (entry-count) `LogsSinceLast` policy. The
/// entry-count cadence is blind to payload size — under 50 KB variables a batched
/// entry is ~1 MB, so 5000 entries is ~5 GB of log before a snapshot would
/// otherwise fire. Bounding by *bytes* keeps the committed log (and thus RSS)
/// bounded under large payloads. `0` disables the byte trigger.
/// (`NANOBPMN_RAFT_SNAPSHOT_BYTES`, default 128 MiB.)
fn snapshot_bytes_threshold() -> i64 {
    raft_env_u64("NANOBPMN_RAFT_SNAPSHOT_BYTES", 128 * 1024 * 1024) as i64
}

/// How often the compaction governor evaluates each partition for a byte-based or
/// quiescence-triggered snapshot. (`NANOBPMN_RAFT_COMPACT_TICK_MS`, default 5 s;
/// `0` disables the governor entirely.)
fn compaction_tick_ms() -> u64 {
    raft_env_u64("NANOBPMN_RAFT_COMPACT_TICK_MS", 5000)
}

/// Upper bound on commands coalesced into a single Raft log entry. Caps per-entry
/// apply work and entry size; under a steady flood the batch fills toward this and
/// openraft's per-entry overhead is amortized across the whole batch.
const MAX_PROPOSE_BATCH: usize = 1024;

/// Byte budget for a single coalesced Raft log entry, capping the propose
/// batcher in addition to [`MAX_PROPOSE_BATCH`] (a count). Under large variable
/// payloads (e.g. 50 KB/instance) a count-only batch of 1024 creates forms a
/// ~50 MB entry; openraft may then bundle several such entries into one
/// AppendEntries, whose transfer cannot finish within the ~250 ms RPC timeout —
/// replication collapses and (sharing the stream transport) starves client
/// traffic, wedging producers. Bounding the entry by bytes keeps each entry (and
/// thus each AppendEntries, with `max_payload_entries`) shippable in time. The
/// batch always contains at least its first command, so a lone oversized command
/// still makes progress. Env-tunable (`NANOBPMN_RAFT_MAX_ENTRY_BYTES`, default
/// 1 MiB).
fn max_entry_bytes() -> u64 {
    raft_env_u64("NANOBPMN_RAFT_MAX_ENTRY_BYTES", 1024 * 1024)
}

/// One queued command awaiting placement into a batched Raft entry, plus the
/// one-shot the batcher fulfils with that command's [`ReplicatedItem`] (or a
/// propose error) once the entry commits and applies.
struct Submission {
    item: ReplicatedCommand,
    resp: tokio::sync::oneshot::Sender<Result<ReplicatedItem, ProposeError>>,
}

/// Command intake classification for the propose batcher's two-tier priority.
///
/// Returns `true` for fresh *demand entering* the system — process creation,
/// job **activation** (a poll, which Zeebe likewise does NOT whitelist), and
/// start-event instance dispatch. These take the low-priority lane. Everything
/// else — job/user-task finalization, cancellation, incident resolution, timer
/// and lock-expiry ticks, deploys, and message/signal correlation — is *progress
/// on already-admitted work* and takes the high lane.
///
/// Two properties make this safe in both directions, mirroring Zeebe's
/// `WhiteListedCommands`:
/// - **Drain can't be starved by intake:** completes never sit in the
///   Raft log behind a backlog of creates, so the cluster always frees the
///   resources of work it accepted (which reopens admission).
/// - **Intake can't be starved by drain:** the high lane's volume is bounded by
///   low-lane admission — you cannot complete/correlate more work than you
///   created — so a create can never be permanently starved. Crucially,
///   *activation* is intake, not drain: a flood of empty activation polls from
///   idle workers stays on the low lane and interleaves with creates FIFO
///   instead of monopolising the high lane and starving creation.
///
/// It matches the engine actor's High/Low mailbox (`deepthi::Priority`) one layer
/// down, so the two agree end to end.
fn is_creation_intake(command: &Command) -> bool {
    matches!(
        command,
        Command::CreateInstance { .. }
            | Command::ActivateJobs { .. }
            | Command::DispatchStartInstance { .. }
    )
}

/// Coalesces concurrently-proposed commands for one partition into batched Raft
/// log entries. A single background task drains every submission that queued
/// while the previous `client_write` was in flight into the next entry — classic
/// group commit: end-to-end latency stays one commit round-trip while throughput
/// scales with batch size, because one append + one replication round-trip + one
/// apply hop now carry up to [`MAX_PROPOSE_BATCH`] commands. A lone proposer
/// (tests, deploy) simply forms batches of one — byte-identical to the prior
/// one-command-per-entry path.
///
/// Two lanes give the drain path priority over creation intake (see
/// [`is_creation_intake`]): every batch is filled from the `hi` lane first, so
/// completes always ride the next entry even while a backlog of
/// creates waits in the `lo` lane. Creation is admitted only with the batch
/// capacity the drain path leaves — the log-layer analogue of Zeebe's
/// `WhiteListedCommands`, and the fix for the credit-starvation latch where a
/// create flood at the single FIFO starved job completion.
struct Batcher {
    hi_tx: tokio::sync::mpsc::UnboundedSender<Submission>,
    lo_tx: tokio::sync::mpsc::UnboundedSender<Submission>,
    /// The proposing node's engine: terminal job commands are stamped from it
    /// before entering the log (see [`stamp_for_proposal`]).
    engine: DeepthiHandle,
}

impl Batcher {
    fn spawn(raft: openraft::Raft<RaftConfig>, engine: DeepthiHandle) -> Self {
        let (hi_tx, mut hi_rx) = tokio::sync::mpsc::unbounded_channel::<Submission>();
        let (lo_tx, mut lo_rx) = tokio::sync::mpsc::unbounded_channel::<Submission>();
        let max_bytes = max_entry_bytes();
        tokio::spawn(async move {
            loop {
                // Block until at least one submission is queued on either lane.
                // `biased` polls the high-priority (drain) lane first, so when
                // both lanes have work waiting, the batch starts with drain
                // commands. `else` fires only once BOTH senders have dropped
                // (partition teardown), ending the task.
                let first = tokio::select! {
                    biased;
                    Some(s) = hi_rx.recv() => s,
                    Some(s) = lo_rx.recv() => s,
                    else => break,
                };
                // Track the coalesced entry's payload size so a batch of large
                // (e.g. 50 KB-variable) commands stays within `max_bytes` and the
                // entry remains shippable in one AppendEntries within the RPC
                // timeout. The first command is always included, so a lone command
                // larger than the budget still makes progress.
                let mut batch_bytes = first.item.command.approx_bytes();
                let mut subs = vec![first];
                // Drain ALL pending high-priority (drain) commands into this
                // batch first, bounded by the count and byte caps — so a
                // completion never queues behind a backlog of creates in a later
                // entry.
                while subs.len() < MAX_PROPOSE_BATCH && batch_bytes < max_bytes {
                    match hi_rx.try_recv() {
                        Ok(s) => {
                            batch_bytes += s.item.command.approx_bytes();
                            subs.push(s);
                        }
                        Err(_) => break,
                    }
                }
                // Fill any remaining batch capacity with low-priority creation
                // intake. Under a sustained drain flood creation yields entirely
                // (the intended backpressure); a completion can never outnumber
                // the creates that produced its jobs, so this is self-limiting and
                // does not permanently starve admission.
                while subs.len() < MAX_PROPOSE_BATCH && batch_bytes < max_bytes {
                    match lo_rx.try_recv() {
                        Ok(s) => {
                            batch_bytes += s.item.command.approx_bytes();
                            subs.push(s);
                        }
                        Err(_) => break,
                    }
                }
                let items: Vec<ReplicatedCommand> = subs.iter().map(|s| s.item.clone()).collect();
                let n = items.len();
                match raft.client_write(ReplicatedBatch::new(items)).await {
                    Ok(res) => {
                        let mut out = res.data.items;
                        if out.len() == n {
                            for (s, item) in subs.into_iter().zip(out.drain(..)) {
                                let _ = s.resp.send(Ok(item));
                            }
                        } else {
                            // apply returns exactly one item per command; an arity
                            // mismatch is a bug, surface it rather than mis-pair.
                            for s in subs {
                                let _ = s.resp.send(Err(ProposeError::Other(format!(
                                    "raft batch response arity mismatch ({} != {n})",
                                    out.len()
                                ))));
                            }
                        }
                    }
                    Err(e) => {
                        // Preserve the leadership distinction: a `ForwardToLeader`
                        // (this node is not / no longer the leader) is a retryable
                        // leadership race, not an internal fault — classify it so
                        // the HTTP layer can answer 503 instead of 500. #1306.
                        let err = ProposeError::from_client_write(e);
                        for s in subs {
                            let _ = s.resp.send(Err(err.clone()));
                        }
                    }
                }
            }
        });
        Self {
            hi_tx,
            lo_tx,
            engine,
        }
    }

    async fn submit(&self, command: Command, now: u64) -> Result<ReplicatedItem, ProposeError> {
        let command = stamp_for_proposal(&self.engine, command).await;
        let (resp, rx) = tokio::sync::oneshot::channel();
        // Route fresh creation intake to the low-priority lane; the drain path
        // (completes, fails, ticks, admin) takes the high lane so it
        // is never queued behind a backlog of creates in the Raft log.
        let tx = if is_creation_intake(&command) {
            &self.lo_tx
        } else {
            &self.hi_tx
        };
        tx.send(Submission {
            item: ReplicatedCommand { command, now },
            resp,
        })
        .map_err(|_| ProposeError::Other("raft propose batcher stopped".to_string()))?;
        rx.await.map_err(|_| {
            ProposeError::Other("raft propose batcher dropped the response".to_string())
        })?
    }
}

/// Stamps a terminal job command with the authoritative attribution resolved
/// from the PROPOSING node's engine, before it enters the Raft log (#963).
///
/// Leader-local activation keeps the activation lock (and its worker) only in
/// the leader's RAM, so a follower re-running the replicated command cannot
/// derive the attribution itself. Every command enters the log through the
/// [`Batcher`] (the sole `client_write` caller), so stamping here covers every
/// terminal-job write path by construction: replicas apply the identical
/// stamped command and persist identical terminal events.
async fn stamp_for_proposal(engine: &DeepthiHandle, command: Command) -> Command {
    if !matches!(
        command,
        Command::CompleteJob { .. } | Command::FailJob { .. } | Command::ThrowJobError { .. }
    ) {
        return command;
    }
    engine
        .with(move |journal| {
            let mut command = command;
            journal.engine().stamp_terminal_attribution(&mut command);
            command
        })
        .await
}

/// A Raft-managed partition: the openraft instance plus handles to its stores.
pub struct RaftPartition {
    pub raft: openraft::Raft<RaftConfig>,
    pub node_id: NodeId,
    pub partition_id: u64,
    batcher: Batcher,
    /// Live (non-purged) log byte footprint, published by the durable log store.
    /// `0` for the volatile `MemLogStore` (in-memory/test deployments), which the
    /// compaction governor simply never byte-triggers. Read by the governor to
    /// decide byte-based snapshots — see [`snapshot_bytes_threshold`].
    log_bytes: Arc<AtomicI64>,
    /// Cumulative bytes this member (as leader) has streamed to each replication
    /// target during an `InstallSnapshot`. Shared with the partition's
    /// [`PartitionNetwork`]. Read by the hand-off catch-up loop
    /// ([`snapshot_bytes_sent`](Self::snapshot_bytes_sent)) to extend the deadline
    /// while a snapshot install is actively transferring.
    snapshot_progress: Arc<SnapshotSendProgress>,
    /// The engine actor this member's state machine drives, and the lifecycle
    /// epoch it was bound to (#1473). [`retire`](Self::retire) takes that
    /// engine's lifecycle lock EXCLUSIVE, which retires the epoch and drains
    /// every in-flight apply / snapshot build / install of this member.
    sm_engine: DeepthiHandle,
    sm_epoch: u64,
    /// This member's snapshot directory (durable under the log dir, or a fresh
    /// per-boot temp dir for a volatile-log member). Retired with the member.
    sm_snapshot_dir: PathBuf,
    /// Whether this member's snapshot directory is VOLATILE (a per-boot temp dir
    /// under [`RAFT_SNAPSHOT_DIR_PREFIX`], because the member has no durable log
    /// dir). Only a volatile dir is reclaimed on retire; a durable recovery
    /// directory is never touched.
    sm_volatile_snap: bool,
}

impl RaftPartition {
    /// Boots a single-voter (RF=1) Raft group for `partition_id` on `node_id`,
    /// backed by `engine`, and initializes it so it elects itself leader. The
    /// returned partition is ready to accept [`propose`](Self::propose).
    pub async fn bootstrap_single(
        node_id: NodeId,
        partition_id: u64,
        addr: String,
        engine: DeepthiHandle,
    ) -> anyhow::Result<Self> {
        // RF=1 single voter: the brisk cadence lets the self-election complete
        // promptly (no peers means no real heartbeating), and the shared config
        // also carries the (jittered) snapshot policy so a solo replica compacts
        // its log on the same env-tunable cadence as a group member.
        let config = Arc::new(raft_config(partition_id).validate()?);

        let log_store = MemLogStore::default();
        let stamp_engine = engine.clone();
        let state_machine = Arc::new(PartitionStateMachine::new_temp(engine, partition_id)?);
        let (sm_engine, sm_epoch, sm_snapshot_dir) = state_machine.retirement_handles();
        let snapshot_progress = Arc::new(SnapshotSendProgress::default());
        let network = PartitionNetwork::new(
            Arc::new(NullTransport),
            partition_id,
            snapshot_progress.clone(),
        );
        let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine).await?;

        let mut members = BTreeMap::new();
        members.insert(node_id, BasicNode::new(addr));
        raft.initialize(members).await?;

        let batcher = Batcher::spawn(raft.clone(), stamp_engine);
        Ok(Self {
            raft,
            node_id,
            partition_id,
            batcher,
            log_bytes: Arc::new(AtomicI64::new(0)),
            snapshot_progress,
            sm_engine,
            sm_epoch,
            sm_snapshot_dir,
            // A single in-memory voter anchors its snapshots in a temp dir.
            sm_volatile_snap: true,
        })
    }

    /// Boots a single-voter (RF=1) Raft group whose **log is crash-durable**,
    /// stored under `log_dir` (milestone B). Unlike [`bootstrap_single`], the
    /// replicated log survives a restart: reopening the same `log_dir` replays the
    /// durable entries back through the (volatile) state machine to reconstruct
    /// engine state. `initialize` is skipped when the log already exists, so this
    /// is the same call for a first boot and a recovery boot.
    pub async fn bootstrap_single_durable(
        node_id: NodeId,
        partition_id: u64,
        addr: String,
        engine: DeepthiHandle,
        log_dir: impl AsRef<std::path::Path>,
    ) -> anyhow::Result<Self> {
        let config = Arc::new(raft_config(partition_id).validate()?);

        let log_dir = log_dir.as_ref().to_path_buf();
        let log_store = crate::raft_logstore::RaftLogStore::open(&log_dir)?;
        let log_bytes = log_store.bytes_handle();
        let stamp_engine = engine.clone();
        let state_machine = Arc::new(PartitionStateMachine::new(
            engine,
            partition_id,
            log_dir.join("snapshots"),
            node_id,
            false,
            Arc::new(AtomicU64::new(u64::MAX)),
        )?);
        let (sm_engine, sm_epoch, sm_snapshot_dir) = state_machine.retirement_handles();
        // A durable journal-backed engine already holds every entry its journal
        // recorded: boot from the newer of journal and snapshot so openraft
        // re-applies neither's entries (#1405).
        state_machine.restore_newest_boot_state().await?;
        let snapshot_progress = Arc::new(SnapshotSendProgress::default());
        let network = PartitionNetwork::new(
            Arc::new(NullTransport),
            partition_id,
            snapshot_progress.clone(),
        );
        let raft = openraft::Raft::new(node_id, config, network, log_store, state_machine).await?;

        // A fresh log needs the one-shot membership bootstrap; a recovered log
        // already carries it, so initializing again would be an error.
        if !raft.is_initialized().await? {
            let mut members = BTreeMap::new();
            members.insert(node_id, BasicNode::new(addr));
            raft.initialize(members).await?;
        }

        let batcher = Batcher::spawn(raft.clone(), stamp_engine);
        Ok(Self {
            raft,
            node_id,
            partition_id,
            batcher,
            log_bytes,
            snapshot_progress,
            sm_engine,
            sm_epoch,
            sm_snapshot_dir,
            // Snapshots anchor next to the durable log — never reclaimed on retire.
            sm_volatile_snap: false,
        })
    }

    /// Boots one **voter** of a multi-node Raft group (RF>1, milestone C) over a
    /// shared [`RaftTransport`], without initializing membership. The caller boots
    /// every member, registers their handles with the transport, then calls
    /// [`initialize`](Self::initialize) once on a single member to form the group.
    /// Splitting construction from initialization is required because a voter must
    /// be able to *receive* AppendEntries/Vote before the group is formed.
    ///
    /// `log_dir` selects the log store: `Some(dir)` uses the crash-durable
    /// file-backed [`RaftLogStore`](crate::raft_logstore::RaftLogStore) (one
    /// directory per partition replica), so a voter — leader *or* follower —
    /// recovers its replicated log after a restart instead of losing everything
    /// it had replicated. `None` falls back to the volatile [`MemLogStore`], used
    /// by in-memory deployments and tests. Either way the log is compacted by
    /// snapshots (see [`raft_config`]'s `snapshot_policy`), so it does not grow
    /// without bound.
    pub async fn bootstrap_member(
        node_id: NodeId,
        partition_id: u64,
        engine: DeepthiHandle,
        transport: Arc<dyn RaftTransport>,
        log_dir: Option<std::path::PathBuf>,
        evict_eligible: bool,
    ) -> anyhow::Result<Self> {
        let config = Arc::new(raft_config(partition_id).validate()?);
        // Anchor snapshots next to the durable log when there is one, else a temp
        // dir for the volatile (in-memory-log) deployments.
        let volatile_snap = log_dir.is_none();
        let snapshot_dir = match log_dir.as_ref() {
            Some(dir) => dir.join("snapshots"),
            None => PartitionStateMachine::temp_snapshot_dir(partition_id),
        };
        // Live current-leader signal the state machine's terminal-eviction gate
        // reads. Only meaningful for an evict-eligible follower; kept fresh by a
        // metrics watcher spawned below once the Raft handle exists.
        let leader = Arc::new(AtomicU64::new(u64::MAX));
        let stamp_engine = engine.clone();
        let state_machine = Arc::new(PartitionStateMachine::new(
            engine,
            partition_id,
            snapshot_dir,
            node_id,
            evict_eligible,
            leader.clone(),
        )?);
        let (sm_engine, sm_epoch, sm_snapshot_dir) = state_machine.retirement_handles();
        // Restore the engine from the durable current snapshot (if any) BEFORE
        // openraft is constructed: with a compacted log the snapshot holds the
        // purged prefix, and the state machine already carries its applied
        // metadata, so openraft's `get_initial_state` reconciles cleanly and only
        // replays the post-snapshot log tail. Without this a rejoining node whose
        // log was purged fails to host the partition entirely.
        //
        // A durable log resumes on-disk: the engine was rebuilt from its durable
        // journal, so the NEWER of journal and snapshot wins, with matching
        // applied metadata, and openraft only re-applies entries neither holds
        // (#1405). A volatile (fresh-receiver) member restores its snapshot (if
        // any) and otherwise installs the leader's.
        if log_dir.is_some() {
            state_machine.restore_newest_boot_state().await?;
        } else {
            state_machine.restore_from_current_snapshot().await?;
        }
        let snapshot_progress = Arc::new(SnapshotSendProgress::default());
        let network = PartitionNetwork::new(transport, partition_id, snapshot_progress.clone());
        // One `Raft` handle, two possible log stores. The handle erases the log
        // storage type, so both arms yield the same `RaftPartition`; building the
        // `Raft` inside each arm avoids needing a common concrete store type. The
        // durable arm also captures the store's live-byte handle for the governor;
        // the volatile arm has none (never byte-triggered).
        let (raft, log_bytes) = match log_dir {
            Some(dir) => {
                let log_store = crate::raft_logstore::RaftLogStore::open(dir)?;
                let log_bytes = log_store.bytes_handle();
                let raft =
                    openraft::Raft::new(node_id, config, network, log_store, state_machine).await?;
                (raft, log_bytes)
            }
            None => {
                let log_store = MemLogStore::default();
                let raft =
                    openraft::Raft::new(node_id, config, network, log_store, state_machine).await?;
                (raft, Arc::new(AtomicI64::new(0)))
            }
        };
        let batcher = Batcher::spawn(raft.clone(), stamp_engine);
        // Keep the state machine's leader signal fresh so its terminal-eviction
        // gate flips off the instant this member wins leadership. Only needed for
        // an evict-eligible follower; an owned member never evicts in `apply`.
        if evict_eligible {
            let mut metrics = raft.metrics();
            tokio::spawn(async move {
                loop {
                    let cur = metrics.borrow().current_leader.unwrap_or(u64::MAX);
                    leader.store(cur, Ordering::Relaxed);
                    if metrics.changed().await.is_err() {
                        break; // Raft dropped: the watcher's job is done.
                    }
                }
            });
        }
        Ok(Self {
            raft,
            node_id,
            partition_id,
            batcher,
            log_bytes,
            snapshot_progress,
            sm_engine,
            sm_epoch,
            sm_snapshot_dir,
            sm_volatile_snap: volatile_snap,
        })
    }

    /// Forms the Raft group from `members` (node id → address). Call once, on one
    /// member, after every voter has been booted and registered with the shared
    /// transport. A no-op (skipped) if the group is already initialized.
    pub async fn initialize(&self, members: BTreeMap<NodeId, BasicNode>) -> anyhow::Result<()> {
        if !self.raft.is_initialized().await? {
            self.raft.initialize(members).await?;
        }
        Ok(())
    }

    /// Boots this node's member for `partition_id` as a **fresh receiver**: an
    /// uninitialized, receive-only member with an EMPTY log, a default (never
    /// granted) vote and no committed / applied / snapshot state, so whichever
    /// leader it is handed to replicates its lineage onto a blank slate. A
    /// receiver that came up carrying a prior lineage would present the new
    /// leader with a divergent COMMITTED prefix — a Raft safety violation that
    /// openraft (rightly) asserts on (`committed log should never be truncated`,
    /// issue #1454).
    ///
    /// The single canonical constructor for every receiver path (the hand-off /
    /// promotion rebuild-as-receiver and the boot-deferred receiver). It fails
    /// closed: a member that is not provably empty is shut down and an error is
    /// returned rather than letting it join a group.
    pub async fn bootstrap_fresh_receiver(
        node_id: NodeId,
        partition_id: u64,
        engine: DeepthiHandle,
        transport: Arc<dyn RaftTransport>,
        evict_eligible: bool,
    ) -> anyhow::Result<Self> {
        // No durable log dir: a receiver never resumes an on-disk lineage, and a
        // volatile member anchors its snapshots in a fresh per-boot temp dir, so
        // no prior snapshot can seed its applied state either.
        let part = Self::bootstrap_member(
            node_id,
            partition_id,
            engine,
            transport,
            None,
            evict_eligible,
        )
        .await?;
        if let Err(e) = part.ensure_fresh_receiver_state().await {
            part.raft.shutdown().await.ok();
            return Err(e);
        }
        Ok(part)
    }

    /// Fails unless this member's Raft state is that of a fresh receiver: no log,
    /// a default vote, and nothing committed, applied or snapshotted.
    pub async fn ensure_fresh_receiver_state(&self) -> anyhow::Result<()> {
        let committed = self.raft.with_raft_state(|s| s.committed).await?;
        let m = self.raft.metrics().borrow().clone();
        anyhow::ensure!(
            m.last_log_index.is_none()
                && m.vote == openraft::Vote::default()
                && committed.is_none()
                && m.last_applied.is_none()
                && m.snapshot.is_none(),
            "partition {}: a fresh receiver must start empty, found last_log_index={:?} \
             vote={:?} committed={committed:?} last_applied={:?} snapshot={:?}",
            self.partition_id,
            m.last_log_index,
            m.vote,
            m.last_applied,
            m.snapshot,
        );
        Ok(())
    }

    /// Awaits this node becoming the *established leader* of the group — its
    /// `current_leader` metric reads self — bounded by `timeout`. Returns whether
    /// leadership was observed within the budget.
    ///
    /// A freshly initialized single-voter group elects itself almost immediately,
    /// but under CPU starvation the openraft leader transition can lag well past a
    /// caller's short retry window. The leader-durable self-promote waits here so
    /// it does not return before leadership is real: otherwise the recovery tick's
    /// post-promote hold-down (a fixed tick budget) can expire while the metric
    /// still lags and re-promote the SAME partition at the next epoch — an
    /// unbounded epoch climb under load. Tying the barrier to the leadership event
    /// (not wall-clock ticks) makes the reclaim epoch deterministic.
    pub async fn wait_for_self_leadership(&self, timeout: std::time::Duration) -> bool {
        let mut metrics = self.raft.metrics();
        let deadline = std::time::Instant::now() + timeout;
        loop {
            if metrics.borrow().current_leader == Some(self.node_id) {
                return true;
            }
            let remaining = deadline.saturating_duration_since(std::time::Instant::now());
            if remaining.is_zero() {
                return false;
            }
            // Wake on the next metrics change or when the budget lapses, whichever
            // comes first — no busy-poll.
            if tokio::time::timeout(remaining, metrics.changed())
                .await
                .is_err()
            {
                return metrics.borrow().current_leader == Some(self.node_id);
            }
        }
    }

    /// Adds `node` as a **learner** (non-voting replica) of this group — the
    /// leader-durable path (ADR 0003). A learner receives the replicated log in the
    /// background but does NOT count toward the write quorum, so the leader (sole
    /// voter) acks without waiting for it. Idempotent in effect: re-adding an
    /// existing learner is a cheap no-op error we swallow. `blocking = false` so the
    /// call returns immediately rather than waiting for the learner to catch up —
    /// catch-up proceeds asynchronously, which is the whole point of the tier.
    pub async fn add_learner(&self, node_id: NodeId, node: BasicNode) -> anyhow::Result<()> {
        match self.raft.add_learner(node_id, node, false).await {
            Ok(_) => Ok(()),
            // Already a member (learner or voter): nothing to do.
            Err(e) if e.to_string().contains("already") => Ok(()),
            Err(e) => Err(anyhow::anyhow!("add_learner({node_id}): {e}")),
        }
    }

    /// Adds `node` as a learner and **blocks until it has caught up** to the
    /// leader's log (openraft `blocking = true`), so a subsequent
    /// [`change_voters_to`](Self::change_voters_to) that promotes it to voter
    /// won't stall the group on a lagging replica. Used by the leadership
    /// hand-off: the incumbent leader brings the returning owner fully in sync
    /// as a learner BEFORE transferring the voting membership to it.
    ///
    /// CAUTION: under a sustained high write rate the learner may never catch up
    /// (the log grows faster than replication) and this call can block
    /// indefinitely — callers MUST wrap it in a timeout and quiesce writes for
    /// the partition while it runs (the Phase C hand-off write-gate). Idempotent
    /// against an already-present member.
    pub async fn add_learner_blocking(
        &self,
        node_id: NodeId,
        node: BasicNode,
    ) -> anyhow::Result<()> {
        match self.raft.add_learner(node_id, node, true).await {
            Ok(_) => Ok(()),
            Err(e) if e.to_string().contains("already") => Ok(()),
            Err(e) => Err(anyhow::anyhow!("add_learner_blocking({node_id}): {e}")),
        }
    }

    /// Replaces this group's voting membership with exactly `voters` (openraft
    /// `change_membership(ReplaceAllVoters, retain = true)`). Every current voter
    /// NOT in `voters` is demoted to a **learner** (retained, not removed), and
    /// if the current leader is among the demoted it steps down — this is how the
    /// hand-off transfers leadership to the returning owner without ever forming
    /// a competing group. Every id in `voters` MUST already be a learner of this
    /// group (call [`add_learner_blocking`](Self::add_learner_blocking) first) or
    /// openraft rejects it with `LearnerNotFound`.
    ///
    /// CAUTION: openraft commits this as a two-step joint change; if the leader
    /// loses leadership or crashes between the joint and the final uniform commit
    /// the group is left in the JOINT config (needs a quorum of BOTH the old and
    /// new voter sets). Callers must treat a mid-flight failure as "joint
    /// suspected" and NOT fall back to forming a fresh competing group (Phase D).
    pub async fn change_voters_to(&self, voters: Vec<NodeId>) -> anyhow::Result<()> {
        self.raft
            .change_membership(voters, true)
            .await
            .map(|_| ())
            .map_err(|e| anyhow::anyhow!("change_voters_to: {e}"))
    }

    /// The replication lag (in log entries) of learner/voter `node_id` behind
    /// this leader's last log index, or `None` if we are not the leader or have
    /// no replication record for `node_id` yet. Used by the hand-off to poll a
    /// learner toward zero lag before promoting it to voter. Cheap: reads the
    /// openraft metrics watch.
    pub fn replication_lag(&self, node_id: NodeId) -> Option<u64> {
        let metrics = self.raft.metrics();
        let m = metrics.borrow();
        if m.state != openraft::ServerState::Leader {
            return None;
        }
        let last = m.last_log_index.unwrap_or(0);
        let repl = m.replication.as_ref()?;
        let matched = repl.get(&node_id)?.as_ref().map(|l| l.index).unwrap_or(0);
        Some(last.saturating_sub(matched))
    }

    /// The matched log index of learner/voter `node_id` on this leader — how far
    /// replication (log stream or a completed snapshot install) has durably
    /// carried it — or `None` if we are not the leader, have no replication
    /// record yet, or the target has matched nothing (a snapshot install still in
    /// flight reports `None` here until it lands). Distinct from
    /// [`replication_lag`](Self::replication_lag): the hand-off catch-up watches
    /// this to tell a learner that is genuinely *advancing* (extend the deadline)
    /// from one that has *stalled* (abort early) — lag alone can't, since under a
    /// moving log head a steadily-catching-up learner shows constant lag.
    pub fn learner_matched(&self, node_id: NodeId) -> Option<u64> {
        let metrics = self.raft.metrics();
        let m = metrics.borrow();
        if m.state != openraft::ServerState::Leader {
            return None;
        }
        let repl = m.replication.as_ref()?;
        repl.get(&node_id)?.as_ref().map(|l| l.index)
    }

    /// Cumulative bytes this leader has streamed to `node_id` during an
    /// `InstallSnapshot`, or `None` if no snapshot chunk has been sent to it yet.
    ///
    /// openraft's leader metrics report only a matched `LogId` per target, which
    /// stays `None` for the *entire* snapshot install — so
    /// [`learner_matched`](Self::learner_matched) cannot distinguish a large
    /// install that is actively transferring from a wedged/dead learner. This
    /// byte counter (bumped per acknowledged chunk in the partition network) is
    /// that missing signal: the hand-off catch-up loop watches it to **extend** the
    /// deadline while an install streams, and to detect a genuinely stalled
    /// transfer (bytes stop advancing) — see `evaluate_catchup`.
    pub fn snapshot_bytes_sent(&self, node_id: NodeId) -> Option<u64> {
        self.snapshot_progress.bytes_sent(node_id)
    }

    /// Replicates `command` (stamped with `now`) through the Raft log and applies
    /// it once committed, returning the events it produced. At RF=1 this commits
    /// as soon as the local log write lands. Routed through the per-partition
    /// [`Batcher`], so a flood of concurrent proposes coalesces into batched
    /// entries; a lone proposer forms a batch of one.
    pub async fn propose(&self, command: Command, now: u64) -> anyhow::Result<Vec<Event>> {
        Ok(self.batcher.submit(command, now).await?.events)
    }

    /// Like [`propose`](Self::propose) but returns the full per-command
    /// [`ReplicatedItem`] so the caller can distinguish a successful apply
    /// (events) from an engine rejection (`error`). Used by the server write path
    /// to map 404/409 statuses through the Raft log.
    pub async fn propose_result(
        &self,
        command: Command,
        now: u64,
    ) -> Result<ReplicatedItem, ProposeError> {
        self.batcher.submit(command, now).await
    }

    /// Whether this partition's Raft core has entered `Shutdown` — it has
    /// terminated (e.g. on an unrecoverable storage error) and no longer applies
    /// committed entries, so every instance/job routed here is stranded. A healthy
    /// partition is `Learner`/`Follower`/`Candidate`/`Leader`; only a dead one is
    /// `Shutdown`. Read on-demand from the openraft metrics watch (cheap borrow).
    pub fn is_shutdown(&self) -> bool {
        self.raft.metrics().borrow().state == openraft::ServerState::Shutdown
    }

    /// Fully retires this member (#1473) and returns the engine's lifecycle
    /// lock held EXCLUSIVE.
    ///
    /// `Raft::shutdown()` joins the core and the ticker, but NOT the
    /// state-machine worker's detached `build_snapshot` tasks (vendored openraft
    /// `core/sm/worker.rs` spawns them and never joins them), an `apply` parked
    /// on a durability barrier, or an `install_snapshot` parked on
    /// deserialization / the adoption permit. Every one of those holds the
    /// engine's single lifecycle lock SHARED for its whole run (see
    /// [`DeepthiHandle::lifecycle_enter`]), so retirement is just:
    ///
    /// 1. Shut the Raft core down (no new state-machine work is dispatched).
    /// 2. Take the engine's lifecycle lock EXCLUSIVE (bounded by
    ///    [`RETIRE_DRAIN_BOUND`]). Acquiring it retires this member's epoch and
    ///    waits until no shared holder is in flight; any operation that starts
    ///    later is refused before it touches the engine or the snapshot dir.
    /// 3. For a VOLATILE member (no durable log dir), remove its temp snapshot
    ///    dir — nothing can still be writing into it. A DURABLE snapshot dir is
    ///    a recovery point and is never touched.
    ///
    /// The returned guard must be held by the caller through the rebuild's
    /// engine resolution (health probe, durable reconstruction, journal swap)
    /// so deployment fan-out and any other shared holder cannot interleave with
    /// it; drop it BEFORE constructing the successor member (whose construction
    /// may dispatch applies that enter the lock shared).
    ///
    /// Fails closed (`Err`) when a shared holder is still in flight past the
    /// bound: the caller must NOT rebuild over it — the supervisor retries.
    pub async fn retire(&self) -> std::io::Result<LifecycleExclusive> {
        self.retire_within(RETIRE_DRAIN_BOUND).await
    }

    /// [`retire`](Self::retire) with an explicit drain bound.
    pub async fn retire_within(
        &self,
        bound: std::time::Duration,
    ) -> std::io::Result<LifecycleExclusive> {
        self.raft.shutdown().await.ok();
        let Ok(exclusive) = tokio::time::timeout(bound, self.sm_engine.lifecycle_exclusive()).await
        else {
            if self.sm_volatile_snap {
                tracing::warn!(
                    partition = self.partition_id,
                    dir = ?self.sm_snapshot_dir,
                    "raft: retiring a member with state-machine work still in flight past the \
                     drain bound; leaving its volatile snapshot dir for the next boot's \
                     orphan sweep (#1473)"
                );
            }
            return Err(std::io::Error::other(format!(
                "raft: partition {} still has state-machine work (apply / snapshot build / \
                 install) holding the engine lifecycle lock {}ms after retirement; refusing \
                 to rebuild over it yet (#1473)",
                self.partition_id,
                bound.as_millis(),
            )));
        };
        if self.sm_volatile_snap {
            let _ = std::fs::remove_dir_all(&self.sm_snapshot_dir);
        }
        Ok(exclusive)
    }

    /// The engine actor this member's state machine drives (#1473).
    pub fn sm_engine(&self) -> &DeepthiHandle {
        &self.sm_engine
    }

    /// Whether this member's lifecycle epoch has been retired.
    pub fn is_retired(&self) -> bool {
        self.sm_engine.lifecycle_epoch() != self.sm_epoch
    }

    /// Test-only: this member's snapshot directory (for the volatile-reclaim
    /// assertions).
    #[cfg(test)]
    pub(crate) fn snapshot_dir(&self) -> &Path {
        &self.sm_snapshot_dir
    }

    /// Test-only: whether this member's snapshot dir is volatile (temp-anchored).
    #[cfg(test)]
    pub(crate) fn has_volatile_snapshot_dir(&self) -> bool {
        self.sm_volatile_snap
    }

    /// This partition's live (non-purged) Raft log byte footprint. `0` for a
    /// volatile (in-memory-log) partition. Read by the compaction governor.
    pub fn log_bytes(&self) -> i64 {
        self.log_bytes.load(Ordering::Relaxed)
    }

    /// Whether this group's committed membership is a JOINT config (more than one
    /// voter set) — the transient two-config state openraft passes through during
    /// `change_membership`. A hand-off that fails while joint means the transfer
    /// may be half-applied (needs a quorum of BOTH sets), so the requester must not
    /// fall back to forming a competing group. Read from the metrics watch.
    pub fn in_joint_config(&self) -> bool {
        let metrics = self.raft.metrics();
        let m = metrics.borrow();
        m.membership_config.membership().get_joint_config().len() > 1
    }

    /// Applied-log index and the index the last local snapshot covers, for the
    /// compaction governor. `unsnapshotted = last_applied − snapshot` is the log
    /// tail a snapshot would compact away.
    fn compaction_indices(&self) -> (u64, u64) {
        let metrics = self.raft.metrics();
        let m = metrics.borrow();
        let last_applied = m.last_applied.map(|l| l.index).unwrap_or(0);
        let snapshot = m.snapshot.map(|l| l.index).unwrap_or(0);
        (last_applied, snapshot)
    }
}

/// The set of Raft groups this node hosts, keyed by partition id. A node hosts a
/// group for every partition it is a replica of; the falcon handler looks
/// up the target partition here to feed it an inbound RPC, and the write path
/// looks up the partition to propose through its leader. Empty by default — only
/// populated when per-partition Raft is enabled — so the non-Raft path is
/// untouched.
#[derive(Default)]
pub struct RaftRegistry {
    partitions: Mutex<HashMap<u64, Arc<RaftPartition>>>,
}

impl RaftRegistry {
    pub fn new() -> Arc<Self> {
        Arc::new(Self::default())
    }

    /// Hosts `part`, keyed by its partition id.
    pub fn insert(&self, part: Arc<RaftPartition>) {
        self.partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(part.partition_id, part);
    }

    /// The hosted group for `partition`, if this node replicates it.
    pub fn get(&self, partition: u64) -> Option<Arc<RaftPartition>> {
        self.partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .get(&partition)
            .cloned()
    }

    /// Whether this node hosts no Raft groups (the non-Raft default).
    pub fn is_empty(&self) -> bool {
        self.partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .is_empty()
    }

    /// A snapshot of every hosted partition, ordered by partition id. Used by the
    /// `/debug/raft` diagnostic endpoint to dump per-partition replication indices.
    pub fn all(&self) -> Vec<Arc<RaftPartition>> {
        let mut parts: Vec<Arc<RaftPartition>> = self
            .partitions
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .values()
            .cloned()
            .collect();
        parts.sort_by_key(|p| p.partition_id);
        parts
    }
}

/// Spawns the per-node **compaction governor**: a periodic task that compacts each
/// hosted partition's Raft log beyond what the entry-count `LogsSinceLast` policy
/// achieves, so committed memory stays bounded under large payloads *and* is
/// reclaimed at idle.
///
/// The entry-count snapshot cadence is blind to payload size and only fires while
/// entries are being applied. Under large variable payloads (~1 MB batched
/// entries) that leaves two gaps this governor closes:
///
/// - **Byte cadence:** when a partition's live log exceeds
///   [`snapshot_bytes_threshold`], trigger a snapshot now rather than waiting for
///   `LogsSinceLast` entries — bounding the committed log (hence RSS) by *bytes*.
/// - **Quiescence compaction:** when a partition stops applying (its
///   `last_applied` is unchanged across a tick) but still has an un-snapshotted
///   log tail, trigger a snapshot so an idle node truncates its payload-bearing
///   log instead of pinning it until the next write.
///
/// Triggering is idempotent (openraft coalesces a redundant request) and runs on
/// every member — leader *and* follower — because each compacts its own local log.
/// After the snapshot, openraft purges below `max_in_snapshot_log_to_keep`. A `0`
/// tick interval (`NANOBPMN_RAFT_COMPACT_TICK_MS=0`) disables the governor.
/// Pure decision for the compaction governor: given a partition's applied/snapshot
/// indices, its live log bytes, the byte threshold, and the `last_applied` observed
/// on the previous tick, decide whether to trigger a snapshot now.
///
/// Triggers when there is an un-snapshotted log tail AND either the log has grown
/// past `byte_threshold` (byte cadence) or the partition applied nothing since the
/// previous tick (quiescence). Returns `false` when the log is already fully
/// snapshotted, so an idle-and-already-compacted partition is never re-triggered.
fn should_compact(
    last_applied: u64,
    snapshot: u64,
    log_bytes: i64,
    byte_threshold: i64,
    prev_applied: Option<u64>,
) -> bool {
    if last_applied.saturating_sub(snapshot) == 0 {
        return false;
    }
    let over_bytes = byte_threshold > 0 && log_bytes >= byte_threshold;
    let quiescent = prev_applied == Some(last_applied);
    over_bytes || quiescent
}

pub fn spawn_compaction_governor(registry: Arc<RaftRegistry>) {
    let tick_ms = compaction_tick_ms();
    if tick_ms == 0 {
        return;
    }
    let byte_threshold = snapshot_bytes_threshold();
    tokio::spawn(async move {
        let mut interval = tokio::time::interval(std::time::Duration::from_millis(tick_ms));
        interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        // Per-partition `last_applied` observed on the previous tick, to detect
        // quiescence (a partition that applied nothing since last tick).
        let mut prev_applied: HashMap<u64, u64> = HashMap::new();
        loop {
            interval.tick().await;
            for part in registry.all() {
                let pid = part.partition_id;
                let (last_applied, snapshot) = part.compaction_indices();
                let was = prev_applied.insert(pid, last_applied);
                if should_compact(
                    last_applied,
                    snapshot,
                    part.log_bytes(),
                    byte_threshold,
                    was,
                ) {
                    // Best-effort: a redundant or in-flight trigger is a no-op, and
                    // a transient error (e.g. mid-election) is retried next tick.
                    let _ = part.raft.trigger().snapshot().await;
                }
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use nanobpmn_engine_core::ProcessBuilder;

    use super::*;

    fn deploy_command() -> Command {
        let proc = ProcessBuilder::new("p")
            .start_event("s")
            .end_event("e")
            .connect("s", "e")
            .build()
            .expect("valid process");
        Command::DeployProcess(proc)
    }

    #[test]
    fn engine_error_status_maps_by_key_mutation_rejections() {
        // #1306: the single source of truth that lets every durable by-key
        // mutation routed through `apply_partition_command` surface the SAME HTTP
        // status whether it was applied directly or replicated through Raft.
        use nanobpmn_engine_core::EngineError as E;
        let cases: &[(E, u16)] = &[
            (E::InstanceNotFound { instance_key: 1 }, 404),
            (
                E::InstanceTransitionInvalid {
                    instance_key: 1,
                    from: "Terminated",
                    to: "Suspended",
                },
                409,
            ),
            (E::IncidentNotFound { incident_key: 1 }, 404),
            (
                E::IncidentNotResolvable {
                    incident_key: 1,
                    reason: "no retries".into(),
                },
                409,
            ),
            (E::ScopeNotFound { scope_key: 1 }, 400),
            (E::UserTaskNotFound { user_task_key: 1 }, 404),
            (E::UserTaskNotActive { user_task_key: 1 }, 409),
            (E::UserTaskAlreadyAssigned { user_task_key: 1 }, 409),
            (
                E::AdHocSubProcessNotFound {
                    ad_hoc_instance_key: 1,
                },
                404,
            ),
            (
                E::AdHocUnknownElement {
                    instance_key: 1,
                    element_id: "x".into(),
                },
                404,
            ),
            (
                E::AdHocNoActivationTargets {
                    ad_hoc_instance_key: 1,
                },
                400,
            ),
            (
                E::TargetProcessDefinitionNotFound {
                    process_definition_key: 1,
                },
                404,
            ),
            (
                E::MappingSourceElementNotFound {
                    instance_key: 1,
                    element_id: "a".into(),
                },
                400,
            ),
            (
                E::UnsupportedMigration {
                    instance_key: 1,
                    element_id: "e".into(),
                    reason: "x".into(),
                },
                409,
            ),
            // Open-parallel/inclusive-join migration rejections (deadlock /
            // early-fire): centralizing the mapping here must not drop the
            // coverage the deleted `open_parallel_join_rejections_are_conflicts`
            // test gave these two variants.
            (
                E::MigratedParallelJoinArityChanged {
                    instance_key: 1,
                    source_element_id: "a".into(),
                    target_element_id: "b".into(),
                    source_incoming_count: 2,
                    target_incoming_count: 3,
                },
                409,
            ),
            (
                E::MigratedJoinFlowMissing {
                    instance_key: 1,
                    source_element_id: "a".into(),
                    target_element_id: "b".into(),
                    flow_source_element_id: "c".into(),
                    flow_ordinal: 0,
                },
                409,
            ),
            (
                E::MigratedGatewayRouteMissing {
                    instance_key: 1,
                    source_element_id: "a".into(),
                    target_element_id: "b".into(),
                    flow_index: 0,
                },
                409,
            ),
        ];
        for (err, want) in cases {
            assert_eq!(
                engine_error_status(err).0,
                *want,
                "{err:?} must map to {want}"
            );
        }
    }

    #[test]
    fn engine_error_status_maps_modify_migrate_rejections() {
        // #1374: every modification / migration rejection the engine surface
        // defines maps to its Zeebe status class here, once — never the 500
        // fallback.
        use nanobpmn_engine_core::EngineError as E;
        let pid = || "p".to_string();
        let cases: Vec<(E, u16)> = vec![
            (
                E::ElementNotFound {
                    instance_key: 1,
                    element_id: "x".into(),
                },
                400,
            ),
            (
                E::ElementInstanceNotFound {
                    instance_key: 1,
                    element_instance_key: 2,
                },
                400,
            ),
            (
                E::ModificationInstructionNotYetSupported {
                    instance_key: 1,
                    instruction: "move_instructions",
                },
                400,
            ),
            (
                E::ModificationInstructionInvalid {
                    instance_key: 1,
                    process_id: pid(),
                    reason: "r".into(),
                },
                400,
            ),
            (
                E::AmbiguousFlowScope {
                    instance_key: 1,
                    process_id: pid(),
                    element_id: "x".into(),
                },
                400,
            ),
            (
                E::AncestorScopeNotFound {
                    instance_key: 1,
                    process_id: pid(),
                    ancestor_element_instance_key: 9,
                },
                400,
            ),
            (
                E::AncestorNotAnAncestor {
                    instance_key: 1,
                    process_id: pid(),
                    element_id: "x".into(),
                    ancestor_element_instance_key: 9,
                },
                400,
            ),
            (
                E::ElementNotActivatable {
                    instance_key: 1,
                    process_id: pid(),
                    element_id: "x".into(),
                    reason: "r".into(),
                },
                400,
            ),
            (
                E::InstanceNotActive {
                    instance_key: 1,
                    process_id: pid(),
                    state: "SUSPENDED",
                },
                409,
            ),
            (
                E::MigrationFlowScopeChanged {
                    instance_key: 1,
                    element_id: "x".into(),
                    source_flow_scope_id: None,
                    target_flow_scope_id: Some("sub".into()),
                },
                409,
            ),
            (
                E::MigrationMultiInstanceMismatch {
                    instance_key: 1,
                    source_element_id: "a".into(),
                    target_element_id: "b".into(),
                },
                409,
            ),
            (
                E::MigrationCatchEventDetached {
                    instance_key: 1,
                    element_id: "a".into(),
                    target_element_id: "b".into(),
                    catch_event_id: "t1".into(),
                    target_catch_event_id: "t2".into(),
                },
                409,
            ),
            (
                E::MigrationCatchEventsMerged {
                    instance_key: 1,
                    element_id: "a".into(),
                    target_catch_event_id: "t2".into(),
                    source_catch_event_ids: vec!["t1".into(), "t1b".into()],
                },
                409,
            ),
            (
                E::MigrationCatchEventSubscriptionFailed {
                    instance_key: 1,
                    element_id: "a".into(),
                    target_element_id: "b".into(),
                    reason: "boom".into(),
                },
                409,
            ),
            (
                E::StartEventExpressionInvalid {
                    process_id: "p".into(),
                    element_id: "s".into(),
                    reason: "boom".into(),
                },
                400,
            ),
        ];
        for (err, want) in cases {
            let (got, msg) = engine_error_status(&err);
            assert_eq!(got, want, "{err:?} -> {got} ({msg})");
            assert_eq!(msg, err.to_string());
        }
    }

    #[test]
    fn engine_error_status_for_command_preserves_complete_job_adhoc_contract() {
        // #1308: `AdHocUnknownElement` is status-polymorphic across endpoints —
        // 404 for `ActivateAdHocActivities` (an unknown container/element is
        // "not found"), but 400 for `CompleteJob` (the agentic completion
        // referenced an unknown activation target — invalid data). The shared
        // command-agnostic `engine_error_status` cannot serve both, so the
        // replicated apply path keys the mapping on the command kind. Without
        // this, a replicated `CompleteJob` hitting `AdHocUnknownElement` is
        // mis-reported as 404 "job not found", falsely claiming the job is
        // missing — the exact divergence from the non-Raft direct contract.
        use nanobpmn_engine_core::EngineError as E;

        let adhoc_unknown = E::AdHocUnknownElement {
            instance_key: 1,
            element_id: "x".into(),
        };
        // Default / ActivateAdHocActivities contract: 404.
        assert_eq!(engine_error_status(&adhoc_unknown).0, 404);
        assert_eq!(
            engine_error_status_for("activate_ad_hoc_sub_process_activities", &adhoc_unknown).0,
            404,
        );
        // CompleteJob contract: 400 (invalid data), matching `complete_job_impl`.
        assert_eq!(
            engine_error_status_for("complete_job", &adhoc_unknown).0,
            400
        );

        // The rest of the `CompleteJob` ad-hoc / business-id direct contract the
        // shared mapping would otherwise drop to 500 (unmapped) or mis-route.
        let complete_job_cases: &[(E, u16)] = &[
            (E::AdHocActivateWithCompletion { job_key: 1 }, 400),
            (E::BusinessIdInvalid { chars: 0 }, 400),
            (E::BusinessIdOnChildInstance { instance_key: 1 }, 409),
            (E::BusinessIdAlreadyAssigned { instance_key: 1 }, 409),
        ];
        for (err, want) in complete_job_cases {
            assert_eq!(
                engine_error_status_for("complete_job", err).0,
                *want,
                "complete_job {err:?} must map to {want}"
            );
        }

        // A command-kind-agnostic error (e.g. `JobNotFound`) is unaffected by the
        // override and still maps identically through both functions.
        let job_not_found = E::JobNotFound { job_key: 7 };
        assert_eq!(
            engine_error_status_for("complete_job", &job_not_found).0,
            engine_error_status(&job_not_found).0,
        );
    }

    #[test]
    fn propose_error_classifies_forward_to_leader_as_retryable_leader() {
        // #1306: a non-leader `client_write` surfaces openraft's `ForwardToLeader`,
        // which `from_client_write` must classify as the retryable
        // `ProposeError::Leader` (→ HTTP 503) — not collapse to `Other` (→ 500).
        // This guards the classification the forwarded-503 contract relies on.
        use openraft::error::{ClientWriteError, ForwardToLeader, RaftError};

        let fwd = RaftError::APIError(ClientWriteError::ForwardToLeader(ForwardToLeader {
            leader_id: Some(2),
            leader_node: Some(BasicNode {
                addr: "127.0.0.1:9002".into(),
            }),
        }));
        assert!(
            matches!(ProposeError::from_client_write(fwd), ProposeError::Leader),
            "ForwardToLeader must classify as the retryable Leader variant"
        );

        // A leadership hint with no known leader is still a leadership race.
        let fwd_no_leader = RaftError::APIError(
            ClientWriteError::<NodeId, BasicNode>::ForwardToLeader(ForwardToLeader {
                leader_id: None,
                leader_node: None,
            }),
        );
        assert!(
            matches!(
                ProposeError::from_client_write(fwd_no_leader),
                ProposeError::Leader
            ),
            "ForwardToLeader with no leader hint must still classify as Leader"
        );

        // A non-leader fatal error (a change-membership conflict) is NOT a
        // leadership race: it must fall to `Other`, not be mislabeled retryable.
        use openraft::error::{ChangeMembershipError, InProgress};
        let fatal = RaftError::APIError(ClientWriteError::<NodeId, BasicNode>::from(
            ChangeMembershipError::InProgress(InProgress {
                committed: None,
                membership_log_id: None,
            }),
        ));
        assert!(
            matches!(
                ProposeError::from_client_write(fatal),
                ProposeError::Other(_)
            ),
            "a non-ForwardToLeader error must classify as Other, not Leader"
        );
    }

    #[test]
    fn should_compact_only_with_unsnapshotted_tail() {
        let thresh = 128 * 1024 * 1024;
        // Fully snapshotted -> never compact, even if quiescent or over bytes.
        assert!(!should_compact(100, 100, thresh + 1, thresh, Some(100)));
        // Un-snapshotted tail + over byte threshold -> compact (byte cadence).
        assert!(should_compact(200, 100, thresh, thresh, None));
        // Under byte threshold but not quiescent (applied advanced) -> hold.
        assert!(!should_compact(200, 100, thresh - 1, thresh, Some(150)));
        // Quiescent (applied unchanged since last tick) with a tail -> compact.
        assert!(should_compact(200, 100, 0, thresh, Some(200)));
        // First observation (no prior) under threshold, not quiescent -> hold.
        assert!(!should_compact(200, 100, 0, thresh, None));
        // Byte threshold disabled (0): only quiescence triggers.
        assert!(!should_compact(200, 100, i64::MAX, 0, Some(150)));
        assert!(should_compact(200, 100, i64::MAX, 0, Some(200)));
    }

    #[test]
    fn reap_superseded_snapshots_removes_stale_artifacts_and_keeps_current() {
        // #1488: the prompt-reaping helper. A snapshot dir holding the current
        // snapshot plus the stale shapes a real replica accrues — an un-tagged
        // pre-#1473 file, a predecessor-generation file, a same-generation
        // orphan — must be reduced to just the current file, while the durable
        // pointer and foreign files survive.
        let dir = std::env::temp_dir().join(format!(
            "nano-reap-test-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .map(|d| d.as_nanos())
                .unwrap_or(0)
        ));
        std::fs::create_dir_all(&dir).expect("create snapshot dir");
        let keep = dir.join("snap-installed-g3-9.bin");
        let stale = [
            dir.join("snap-7.bin"),              // pre-#1473 un-tagged
            dir.join("snap-g1-3.bin"),           // predecessor generation
            dir.join("snap-installed-g2-8.bin"), // superseded install
            dir.join("snap-g3-8.bin"),           // same-generation orphan
        ];
        for path in stale.iter().chain(std::iter::once(&keep)) {
            std::fs::write(path, b"snapshot-bytes").expect("write snapshot file");
        }
        let ptr = dir.join("current-snapshot.json");
        std::fs::write(&ptr, b"{}").expect("write pointer");
        let foreign = dir.join("notes.txt");
        std::fs::write(&foreign, b"not ours").expect("write foreign file");
        let incoming = dir.join(format!("incoming-{}-0.tmp", std::process::id()));
        std::fs::write(&incoming, b"partial").expect("write in-flight partial");

        let in_flight = Arc::new(Mutex::new(std::collections::HashSet::new()));
        // A file a concurrent build is still writing (registered in flight)
        // must survive the reap; once its guard drops it is reapable again.
        let building = dir.join("snap-g3-10.bin");
        std::fs::write(&building, b"partial build").expect("write in-flight build");
        let guard = InFlightSnapshot::register(&in_flight, &building);
        // The streaming writer's `.tmp` staging file (snapstream.rs renames it
        // into place) is registered alongside its final path and must likewise
        // survive the reap — it matches the `snap-` prefix, so an unregistered
        // one would be deleted out from under the writer's reopen/rename.
        let staging = dir.join("snap-g3-10.tmp");
        std::fs::write(&staging, b"partial build staging").expect("write staging file");
        let staging_guard = InFlightSnapshot::register(&in_flight, &staging);
        // Likewise an install's promoted-but-unpublished file.
        let promoted = dir.join("snap-installed-g3-11.bin");
        std::fs::write(&promoted, b"promoted").expect("write promoted install");
        let promoted_guard = InFlightSnapshot::register(&in_flight, &promoted);

        reap_superseded_snapshots(&dir, &keep, &in_flight, 0, 0);
        assert!(building.exists(), "an in-flight build is never reaped");
        assert!(
            staging.exists(),
            "an in-flight build's .tmp staging file is never reaped"
        );
        assert!(promoted.exists(), "an unpublished install is never reaped");
        drop(guard);
        drop(staging_guard);
        drop(promoted_guard);
        assert!(
            in_flight
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .is_empty(),
            "guards deregister on drop"
        );
        reap_superseded_snapshots(&dir, &keep, &in_flight, 0, 0);
        assert!(
            !building.exists(),
            "a superseded build is reaped once published"
        );
        assert!(
            !staging.exists(),
            "a leftover .tmp staging file is reaped once its guard drops"
        );
        assert!(
            !promoted.exists(),
            "a superseded install is reaped once published"
        );

        assert!(keep.exists(), "the current snapshot survives");
        for path in &stale {
            assert!(!path.exists(), "stale artifact reaped: {path:?}");
        }
        assert!(ptr.exists(), "the durable pointer is never reaped");
        assert!(foreign.exists(), "a foreign file is never reaped");
        assert!(
            incoming.exists(),
            "an in-flight receive is never reaped out from under openraft"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn a_build_reap_never_deletes_a_concurrently_published_install() {
        // #1488 review regression for the interleaving: a local build's sweep
        // runs while a follower install has published and returned (dropping
        // its in-flight guard); the build's `keep` names only its OWN file, so
        // without the per-partition snapshot publication lock the sweep deletes
        // the install's file even though `current-snapshot.json` references it
        // — leaving recovery without that snapshot after the covered log is
        // purged. The test drives the exact interleaving deterministically: a
        // barrier parks the build just BEFORE it takes the publication lock,
        // the install publishes and returns (dropping its guard), and only
        // then is the build released into its publish+sweep.
        let engine = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        engine
            .with(|j| {
                let _ = j
                    .apply_command_at(deploy_command(), 1)
                    .expect("deploy applies");
            })
            .await;
        let mut sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(engine.clone(), 0).expect("snapshot dir"));
        let dir = sm.snapshot_dir.clone();

        // The install's snapshot body comes from a throwaway builder on the
        // same engine; its published file is the stale artifact the racing
        // sweep later reaps.
        let mut donor_builder = sm.get_snapshot_builder().await;
        let mut donor = donor_builder.build_snapshot().await.expect("donor build");
        let donor_meta = donor.meta.clone();
        let mut donor_bytes = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            donor
                .snapshot
                .read_to_end(&mut donor_bytes)
                .await
                .expect("read donor snapshot");
        }
        let donor_file = sm
            .current_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| s.path.clone())
            .expect("the donor build published");
        assert!(
            donor_file.exists(),
            "the donor snapshot is on disk before the race"
        );

        // The build's member is constructed BEFORE the receive starts: a
        // continuation member (shares the receive epoch — recv_idx and the
        // in-flight set — so a later receive is registered for it too), so its
        // construction sweep runs on a dir with no live incoming file.
        let build_engine = engine.clone();
        let build_member = Arc::new(
            sm.new_continuation_for_test(build_engine)
                .expect("continuation snapshot dir"),
        );

        // The receive starts now — registered in flight on the shared set, so
        // the build's publish sweep below cannot delete it mid-stream (the
        // second regression this round covers).
        let mut received = sm
            .begin_receiving_snapshot()
            .await
            .expect("begin receiving snapshot");
        {
            use tokio::io::AsyncWriteExt;
            received
                .write_all(&donor_bytes)
                .await
                .expect("write snapshot body");
            received.flush().await.expect("flush snapshot body");
        }

        // The racing build: parks at the barrier just before its publish.
        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let build_barrier = Arc::clone(&barrier);
        let build = tokio::spawn(async move {
            let mut sm = build_member;
            let mut builder = sm.get_snapshot_builder().await;
            builder.test_writer_barrier = Some(build_barrier);
            builder.build_snapshot().await.expect("racing build");
            sm
        });

        // The install publishes and returns while the build is parked before
        // its own publish. Its adoption is skipped: this replica shares the
        // engine whose state the snapshot was captured from, and the
        // checkpoint permit the adoption would take is held by the paused
        // build's own capture path — the race under test is the PUBLISH side
        // (pointer + reap), which runs in full.
        let install_sm = sm.clone();
        let install_meta = donor_meta.clone();
        let install = tokio::spawn(async move {
            install_sm
                .install_snapshot_skip_adoption_for_test(&install_meta, received)
                .await
                .expect("install snapshot");
            install_sm
        });
        let install_sm = install.await.expect("install task");
        let installed = install_sm
            .current_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| s.path.clone())
            .expect("the install published a current snapshot");
        assert!(
            installed.exists(),
            "the installed snapshot is on disk before the build's sweep"
        );
        assert!(
            !install_sm
                .in_flight_snapshots
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .contains(&installed),
            "the install's in-flight guard dropped when it returned"
        );

        // Release the build into its publish+sweep: the exact interleaving
        // that used to delete the published install — the build's `keep`
        // names only its own file and the install's guard is gone, so only
        // the publication lock (which serializes the install's publish with
        // this sweep) keeps the pointer's file alive.
        barrier.wait().await;
        let build_sm = build.await.expect("build task");

        // The build is newer, so it wins the pointer; the install's file is
        // reaped as superseded. The invariant under test: the pointer names
        // the build's file and that file is physically present — recovery
        // never references a reaped file.
        let ptr = read_snapshot_ptr(&dir).expect("a durable pointer is published");
        let build_file = build_sm
            .current_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| s.path.clone())
            .expect("the build published");
        assert_eq!(
            ptr.path, build_file,
            "the durable pointer names the newer build's snapshot"
        );
        assert!(
            ptr.path.exists(),
            "the pointer's snapshot file survives every reap"
        );
        assert!(
            !installed.exists(),
            "the superseded install is reaped once the newer build publishes"
        );
        assert!(
            !donor_file.exists(),
            "the superseded donor snapshot is reaped"
        );
    }

    /// Shared fixture for the publication-race regressions (#1488 review): a
    /// replica with a published donor snapshot, a continuation member to run
    /// the racing build on, and a received (not yet installed) copy of the
    /// donor body.
    async fn publication_race_fixture() -> (
        Arc<PartitionStateMachine>,
        PathBuf,
        SnapshotMeta<NodeId, BasicNode>,
        Arc<PartitionStateMachine>,
        Box<SnapshotFile>,
    ) {
        let engine = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        engine
            .with(|j| {
                let _ = j
                    .apply_command_at(deploy_command(), 1)
                    .expect("deploy applies");
            })
            .await;
        let mut sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(engine.clone(), 0).expect("snapshot dir"));
        let dir = sm.snapshot_dir.clone();
        let mut donor_builder = sm.get_snapshot_builder().await;
        let mut donor = donor_builder.build_snapshot().await.expect("donor build");
        let donor_meta = donor.meta.clone();
        let mut donor_bytes = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            donor
                .snapshot
                .read_to_end(&mut donor_bytes)
                .await
                .expect("read donor snapshot");
        }
        let build_member = Arc::new(
            sm.new_continuation_for_test(engine)
                .expect("continuation snapshot dir"),
        );
        let mut received = sm
            .begin_receiving_snapshot()
            .await
            .expect("begin receiving snapshot");
        {
            use tokio::io::AsyncWriteExt;
            received
                .write_all(&donor_bytes)
                .await
                .expect("write snapshot body");
            received.flush().await.expect("flush snapshot body");
        }
        (sm, dir, donor_meta, build_member, received)
    }

    #[tokio::test]
    async fn a_competing_publisher_waits_for_the_publication_lock() {
        // #1488 review regression: park a build INSIDE the publication lock
        // (pointer + cache published, reap not yet run) and prove a concurrent
        // install cannot publish until the build releases the lock. Removing
        // either `snapshot_publish_lock` acquisition makes the install finish
        // inside the window and fails this test.
        let (sm, dir, donor_meta, build_member, received) = publication_race_fixture().await;
        let reached = Arc::new(tokio::sync::Barrier::new(2));
        let release = Arc::new(tokio::sync::Barrier::new(2));
        let (b_reached, b_release) = (Arc::clone(&reached), Arc::clone(&release));
        let build = tokio::spawn(async move {
            let mut sm = build_member;
            let mut builder = sm.get_snapshot_builder().await;
            builder.test_publish_barrier = Some((b_reached, b_release));
            builder.build_snapshot().await.expect("racing build");
            sm
        });
        reached.wait().await;

        let install_sm = sm.clone();
        let install = tokio::spawn(async move {
            install_sm
                .install_snapshot_skip_adoption_for_test(&donor_meta, received)
                .await
                .expect("install snapshot");
        });
        tokio::time::sleep(std::time::Duration::from_millis(200)).await;
        assert!(
            !install.is_finished(),
            "the install must not publish while the build holds the publication lock"
        );
        release.wait().await;
        let _build_sm = build.await.expect("build task");
        install.await.expect("install task");

        let ptr = read_snapshot_ptr(&dir).expect("a durable pointer is published");
        assert!(
            ptr.path.exists(),
            "the pointer's snapshot file survives every reap"
        );
    }

    #[tokio::test]
    async fn an_older_build_never_regresses_a_newer_installed_snapshot() {
        // #1488 review regression: a build captured at index N reaches
        // publication after an install at M > N published. The build must not
        // replace the pointer/cache nor reap the installed file (OpenRaft may
        // already purge through M); it hands back the newer snapshot instead.
        let (sm, dir, donor_meta, _build_member, received) = publication_race_fixture().await;
        let mut newer_meta = donor_meta.clone();
        // The donor build captured no applied log id (None); any Some(..) is
        // strictly newer than both it and the racing build's capture.
        newer_meta.last_log_id = Some(LogId::new(openraft::CommittedLeaderId::new(1, 0), 100));
        newer_meta.snapshot_id = format!("{}-newer", newer_meta.snapshot_id);

        let barrier = Arc::new(tokio::sync::Barrier::new(2));
        let build_barrier = Arc::clone(&barrier);
        // Build on the SAME state machine the install publishes to: the
        // non-regression check reads its published-snapshot cache. Capture
        // the builder BEFORE spawning/installing so it deterministically
        // records the pre-install (older) applied index.
        let mut build_sm = sm.clone();
        let mut builder = build_sm.get_snapshot_builder().await;
        builder.test_writer_barrier = Some(build_barrier);
        let build =
            tokio::spawn(async move { builder.build_snapshot().await.expect("racing build") });

        sm.install_snapshot_skip_adoption_for_test(&newer_meta, received)
            .await
            .expect("install snapshot");
        let installed = sm
            .current_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| s.path.clone())
            .expect("the install published");

        barrier.wait().await;
        let built = build.await.expect("build task");
        assert_eq!(
            built.meta.last_log_id, newer_meta.last_log_id,
            "the stale build hands back the newer published snapshot"
        );
        let ptr = read_snapshot_ptr(&dir).expect("a durable pointer is published");
        assert_eq!(ptr.path, installed, "the pointer never regresses");
        assert!(
            installed.exists(),
            "the newer installed snapshot is never reaped"
        );
        let current = sm
            .current_snapshot
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .as_ref()
            .map(|s| s.meta.last_log_id);
        assert_eq!(
            current,
            Some(newer_meta.last_log_id),
            "the cache never regresses"
        );
    }

    #[test]
    fn snapshot_alert_threshold_defaults_positive() {
        // #1488: the alert threshold helper. The env var is read per call; the
        // test env never sets it, so this asserts the documented default (1 GiB,
        // ~2000x the healthy per-partition snapshot size observed in the #100
        // soak) — high enough that ordinary growth never pages, low enough that
        // a multi-GB follower blowup fires immediately.
        assert_eq!(
            snapshot_alert_bytes_threshold(),
            1024 * 1024 * 1024,
            "the default alert ceiling is 1 GiB"
        );
    }

    #[tokio::test]
    async fn install_snapshot_publishes_size_metric_and_reaps_superseded_files() {
        // #1488 end-to-end at the state-machine surface: a follower installing a
        // snapshot publishes the per-partition/per-replica size series AND
        // promptly reaps the superseded snapshot files already in its dir.
        let src = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        src.with(|j| {
            let _ = j
                .apply_command_at(deploy_command(), 1)
                .expect("deploy applies");
        })
        .await;
        let mut src_sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(src.clone(), 0).expect("snapshot dir"));
        let mut builder = src_sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.expect("build snapshot");
        let mut reader = snap.snapshot;
        let mut bytes = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            reader
                .read_to_end(&mut bytes)
                .await
                .expect("read snapshot body");
        }

        // The follower: an empty replica whose snapshot dir already holds a
        // superseded artifact (as a lagging follower's would after earlier
        // installs / crashed receives). Its replica label is unique to this
        // test, so neither the source build's `{node="0"}` series nor any
        // parallel test can satisfy (or clobber) the destination assertion.
        const DST_NODE: NodeId = 1_488_550_001;
        let dst = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let mut dst_sm: Arc<PartitionStateMachine> = Arc::new(
            PartitionStateMachine::new(
                dst.clone(),
                0,
                PartitionStateMachine::temp_snapshot_dir(0),
                DST_NODE,
                false,
                Arc::new(AtomicU64::new(u64::MAX)),
            )
            .expect("snapshot dir"),
        );
        let series =
            format!("nanobpm_raft_snapshot_file_bytes{{node=\"{DST_NODE}\",partition=\"0\"}}");
        assert!(
            !crate::metrics::gather().contains(&series),
            "the destination series must be absent before install"
        );
        let stale = dst_sm.snapshot_dir.join("snap-g1-1.bin");
        std::fs::write(&stale, b"stale").expect("write stale snapshot");

        let mut received = dst_sm
            .begin_receiving_snapshot()
            .await
            .expect("begin receiving snapshot");
        {
            use tokio::io::AsyncWriteExt;
            received
                .write_all(&bytes)
                .await
                .expect("write snapshot body");
            received.flush().await.expect("flush snapshot body");
        }
        dst_sm
            .install_snapshot(&snap.meta, received)
            .await
            .expect("install snapshot");

        // The superseded file is reaped promptly, the installed snapshot is the
        // only one left, and the per-partition/per-replica gauge carries the
        // installed file's size for this replica.
        assert!(
            !stale.exists(),
            "the superseded snapshot was reaped at install time"
        );
        let remaining: Vec<_> = std::fs::read_dir(&dst_sm.snapshot_dir)
            .expect("list snapshot dir")
            .flatten()
            .filter_map(|e| {
                e.file_name()
                    .to_str()
                    .map(str::to_owned)
                    .filter(|n| n.starts_with("snap-"))
            })
            .collect();
        assert_eq!(
            remaining.len(),
            1,
            "exactly the installed snapshot remains, got {remaining:?}"
        );
        let gauge = crate::metrics::gather();
        let expected = format!("{series} {}", bytes.len());
        assert!(
            gauge.contains(&expected),
            "the per-replica size series is published ({expected}):\n{gauge}"
        );
    }

    #[test]
    fn snapshot_build_concurrency_defaults_and_floors() {
        // Env-free default is 2 concurrent builds (steady-state co-hosted partitions
        // rarely coincide thanks to the jitter, so a small cap is unobtrusive).
        // The env var is read at process start via the OnceLock, so here we only
        // assert the pure default + floor of the helper.
        let c = snapshot_build_concurrency();
        assert!(c >= 1, "concurrency must be floored at 1, got {c}");
        // The semaphore is sized from the same helper, so it always has >=1 permit
        // (acquire_many(concurrency) for the exclusive recovery path can succeed).
        // Probe a freshly constructed one: the process-global instance is shared
        // with concurrently running tests whose snapshot builds hold its permits,
        // so its *available* count is not a capacity measure.
        assert_eq!(new_snapshot_build_sem().available_permits(), c as usize);
    }

    #[test]
    fn snapshot_recovery_engages_on_large_sm_not_small() {
        use std::time::Duration;
        let thresh = snapshot_recovery_bytes_threshold();
        assert!(thresh > 0, "recovery byte threshold must be positive");

        // A lean steady-state snapshot (well under the threshold) leaves the
        // build-concurrency limiter + cadence stretch disengaged.
        crate::metrics::observe_snapshot_build(Duration::ZERO, Duration::ZERO, 1024);
        assert!(
            !snapshot_recovery_engaged(),
            "small SM ({} bytes) must not engage recovery gating (threshold {thresh})",
            crate::metrics::last_snapshot_bytes(),
        );

        // A large resident SM (a returning owner draining a deep reclaim backlog)
        // crosses the threshold and engages both fixes.
        crate::metrics::observe_snapshot_build(Duration::ZERO, Duration::ZERO, thresh + 1);
        assert!(
            snapshot_recovery_engaged(),
            "large SM ({} bytes) must engage recovery gating (threshold {thresh})",
            crate::metrics::last_snapshot_bytes(),
        );

        // Reset the process-global gauge so we don't perturb other tests.
        crate::metrics::observe_snapshot_build(Duration::ZERO, Duration::ZERO, 0);
    }

    #[test]
    fn snapshot_jitter_is_bounded_centered_and_desynchronizes_partitions() {
        let base = 5000u64;
        let pct = 25u64;
        let range = base * pct / 100; // ±1250

        // Bounded: every partition stays within ± range of the base.
        let vals: Vec<u64> = (0..12)
            .map(|p| jitter_snapshot_logs(base, pct, p))
            .collect();
        for (p, &v) in vals.iter().enumerate() {
            assert!(
                v >= base - range && v <= base + range,
                "partition {p} jittered to {v}, outside [{}, {}]",
                base - range,
                base + range
            );
        }

        // Deterministic: same inputs → same output.
        assert_eq!(vals[3], jitter_snapshot_logs(base, pct, 3));

        // Desynchronizes: the 12 co-hosted partitions do not all share one
        // threshold — the whole point of the jitter. Expect a wide spread.
        let mut sorted = vals.clone();
        sorted.sort_unstable();
        sorted.dedup();
        assert!(
            sorted.len() >= 10,
            "expected the 12 partitions to spread across distinct thresholds, got {sorted:?}"
        );

        // Roughly centered: the mean offset should be near zero, not skewed to
        // one side (which would defeat the "average cadence unchanged" property).
        let sum: i64 = vals.iter().map(|&v| v as i64 - base as i64).sum();
        let mean = sum / vals.len() as i64;
        assert!(
            mean.abs() < range as i64 / 2,
            "jitter mean {mean} too skewed"
        );

        // pct == 0 disables jitter for an exact, test-pinnable base.
        for p in 0..12 {
            assert_eq!(jitter_snapshot_logs(base, 0, p), base);
        }

        // Never returns 0 even with an absurdly small base (openraft would reject
        // a zero snapshot threshold).
        assert!(jitter_snapshot_logs(1, 90, 7) >= 1);
    }

    #[test]
    fn creation_and_activation_are_low_priority_intake() {
        use std::collections::HashMap;
        // Fresh demand entering the system — process creation AND job activation
        // (a poll) — takes the low-priority lane. Putting activation on the high
        // lane lets a flood of empty polls from idle workers starve creation,
        // which is exactly what Zeebe avoids by NOT whitelisting JobBatch.ACTIVATE.
        assert!(is_creation_intake(&Command::CreateInstance {
            process_id: "p".into(),
            variables: HashMap::new(),
            tags: vec![],
            business_id: None,
            process_definition_key: None,
            version: None,
        }));
        assert!(is_creation_intake(&Command::activate_jobs(
            "t", "w", 1, 1, 0
        )));

        // The drain / progress path (finalization, cancellation, maintenance)
        // takes the high-priority lane; its volume is bounded by low-lane
        // admission, so it can never permanently starve a create.
        assert!(!is_creation_intake(&Command::complete_job_with(
            1,
            HashMap::new()
        )));
        assert!(!is_creation_intake(&Command::fail_job(1, 0, "e")));
        assert!(!is_creation_intake(&Command::ExpireJobs { now: 0 }));
        assert!(!is_creation_intake(&Command::TriggerTimers { now: 0 }));
        assert!(!is_creation_intake(&Command::CancelInstance {
            instance_key: 1
        }));
        assert!(!is_creation_intake(&deploy_command()));
    }

    /// A fresh receiver is provably empty, and the emptiness guard fails closed
    /// on a member that carries a lineage (issue #1454): a receiver with a
    /// committed prefix would present a new leader with a divergent committed
    /// lineage.
    #[tokio::test]
    async fn fresh_receiver_starts_empty_and_the_guard_rejects_a_lineage() {
        let fresh = RaftPartition::bootstrap_fresh_receiver(
            1,
            0,
            DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None),
            Arc::new(NullTransport),
            false,
        )
        .await
        .expect("a fresh receiver boots empty");
        fresh
            .ensure_fresh_receiver_state()
            .await
            .expect("fresh receiver state is empty");
        fresh.raft.shutdown().await.ok();

        // An initialized single voter carries a log, a vote and a committed entry.
        let led = RaftPartition::bootstrap_single(
            0,
            0,
            "http://self".into(),
            DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None),
        )
        .await
        .expect("bootstrap single-voter raft");
        let mut rejected = false;
        for _ in 0..300 {
            if led.ensure_fresh_receiver_state().await.is_err() {
                rejected = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(rejected, "a member with a lineage is not a fresh receiver");
        led.raft.shutdown().await.ok();
    }

    #[tokio::test]
    async fn single_voter_replicates_and_applies_a_command() {
        let part = RaftPartition::bootstrap_single(
            0,
            0,
            "http://self".into(),
            DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None),
        )
        .await
        .expect("bootstrap single-voter raft");

        // Deploy through the Raft log, then create an instance through it.
        let deploy_events = part.propose(deploy_command(), 1_000).await.expect("deploy");
        assert!(
            deploy_events
                .iter()
                .any(|e| matches!(e, Event::ProcessDeployed { .. })),
            "the deploy command replicated and applied (got {deploy_events:?})"
        );

        let create_events = part
            .propose(
                Command::CreateInstance {
                    process_id: "p".into(),
                    variables: Default::default(),
                    tags: Vec::new(),
                    business_id: None,
                    process_definition_key: None,
                    version: None,
                },
                2_000,
            )
            .await
            .expect("create");
        assert!(
            create_events
                .iter()
                .any(|e| matches!(e, Event::ProcessInstanceCreated { .. })),
            "the create command replicated and applied (got {create_events:?})"
        );

        // The Raft metrics confirm this node is the leader of a committed log.
        let metrics = part.raft.metrics().borrow().clone();
        assert_eq!(metrics.current_leader, Some(0));
        assert!(
            metrics.last_applied.map(|l| l.index).unwrap_or(0) >= 2,
            "at least the deploy + create entries committed and applied"
        );

        part.raft.shutdown().await.expect("clean shutdown");
    }

    #[tokio::test]
    async fn snapshot_captures_compact_state_and_installs_into_a_fresh_replica() {
        // A source state machine accrues live state directly through its engine
        // actor (the same effect `apply` has), then snapshots it. It holds one
        // auto-completing instance (start->end "p", terminal on create) AND one
        // that parks on a service-task job (stays Active), so the install path is
        // exercised for both a terminal shell and a live instance.
        let wait_proc = ProcessBuilder::new("w")
            .start_event("s")
            .service_task("t", "work")
            .end_event("e")
            .connect("s", "t")
            .connect("t", "e")
            .build()
            .expect("valid process");
        let src = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        src.with(|j| {
            let _ = j
                .apply_command_at(deploy_command(), 1)
                .expect("deploy applies");
        })
        .await;
        src.with(move |j| {
            let _ = j
                .apply_command_at(Command::DeployProcess(wait_proc), 2)
                .expect("deploy wait proc applies");
        })
        .await;
        src.with(|j| {
            let _ = j
                .apply_command_at(
                    Command::CreateInstance {
                        process_id: "p".into(),
                        variables: Default::default(),
                        tags: Vec::new(),
                        business_id: None,
                        process_definition_key: None,
                        version: None,
                    },
                    3,
                )
                .expect("create applies");
        })
        .await;
        let active_key = src
            .with(|j| {
                let (ev, _) = j
                    .apply_command_at(
                        Command::CreateInstance {
                            process_id: "w".into(),
                            variables: Default::default(),
                            tags: Vec::new(),
                            business_id: None,
                            process_definition_key: None,
                            version: None,
                        },
                        4,
                    )
                    .expect("create applies");
                ev.iter().find_map(|e| e.instance_key()).unwrap()
            })
            .await;

        let mut src_sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(src.clone(), 0).expect("snapshot dir"));
        let mut builder = src_sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.expect("build snapshot");

        // The body is a compact chunked snapshot on disk (#1337), not an event
        // log: streaming it back reassembles a self-contained EngineSnapshot.
        let mut reader = snap.snapshot;
        let mut bytes = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            reader
                .read_to_end(&mut bytes)
                .await
                .expect("read snapshot body");
        }
        let _: nanobpmn_engine_core::EngineSnapshot =
            nano_server_storage::snapstream::read_streaming_engine_snapshot_from(
                std::io::Cursor::new(&bytes),
            )
            .expect("snapshot body is a streaming state capture");

        // A brand-new, empty replica installs the snapshot and ends up with
        // byte-for-byte identical engine state — the cross-node catch-up path.
        // Drive the receive→write→install sequence openraft's chunked transfer
        // performs: begin a receiving file, stream the body in, then install.
        let dst = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let mut dst_sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(dst.clone(), 0).expect("snapshot dir"));
        let mut received = dst_sm
            .begin_receiving_snapshot()
            .await
            .expect("begin receiving snapshot");
        {
            use tokio::io::AsyncWriteExt;
            received
                .write_all(&bytes)
                .await
                .expect("write snapshot body");
            received.flush().await.expect("flush snapshot body");
        }
        dst_sm
            .install_snapshot(&snap.meta, received)
            .await
            .expect("install snapshot");

        let src_state = src.with(|j| j.state().clone()).await;
        let dst_state = dst.with(|j| j.state().clone()).await;
        // The deployed definitions transfer verbatim in the snapshot.
        assert_eq!(
            src_state.processes, dst_state.processes,
            "the deployed definitions transferred in the snapshot"
        );
        assert!(
            !dst_state.processes.is_empty(),
            "the deployed definition transferred in the snapshot"
        );
        // Install sheds the source's terminal shells (they are done and already
        // durable in the read model at the source; they never flow through this
        // replica's exporter), but the live instance transfers intact.
        assert!(
            dst_state.instances.contains_key(&active_key),
            "the in-flight instance transferred and stays resident after install"
        );
        assert_eq!(
            dst_state.instances.len(),
            1,
            "only the in-flight instance is resident; the terminal shell was shed"
        );
    }

    /// #1337 durability proof at the Raft surface: a snapshot built from a
    /// journal whose instances are **cold** (wholly off-heap in the spill store)
    /// must stream them straight from disk — never materializing the cold set in
    /// RAM — and a fresh replica installing that snapshot must recover every cold
    /// instance. Deterministic: the cold spill is forced synchronously, the
    /// snapshot is built and installed through the real chunked transfer, and the
    /// assertions run on the installed state (no sleeps, no retries).
    #[tokio::test]
    async fn snapshot_streams_cold_instances_to_a_fresh_replica() {
        use nanobpmn_engine_core::Value;
        let wait_proc = ProcessBuilder::new("wc")
            .start_event("s")
            .service_task("t", "work")
            .end_event("e")
            .connect("s", "t")
            .connect("t", "e")
            .build()
            .expect("valid process");

        // Source journal with a cold-spill tier wired, three instances forced
        // wholly off-heap.
        let store = Arc::new(
            nano_server_storage::varspill::VarSpillStore::open(None).expect("in-memory store"),
        );
        let mut src_journal = Journal::in_memory_partition(0);
        src_journal.set_cold_spill(Arc::clone(&store), 1, 0);
        let src = DeepthiHandle::spawn(src_journal, 0, None);
        src.with(move |j| {
            let _ = j
                .apply_command_at(Command::DeployProcess(wait_proc), 1)
                .expect("deploy applies");
        })
        .await;
        let mut cold_keys = Vec::new();
        for i in 0..3u64 {
            let key = src
                .with(move |j| {
                    let mut vars = HashMap::new();
                    vars.insert("data".to_string(), Value::Str(format!("cold-{i}")));
                    let (ev, _) = j
                        .apply_command_at(
                            Command::CreateInstance {
                                process_id: "wc".into(),
                                variables: vars,
                                tags: Vec::new(),
                                business_id: None,
                                process_definition_key: None,
                                version: None,
                            },
                            10 + i,
                        )
                        .expect("create applies");
                    ev.iter().find_map(|e| e.instance_key()).unwrap()
                })
                .await;
            cold_keys.push(key);
        }
        // Force every instance wholly off-heap (cold). Precondition for the test:
        // the snapshot must recover instances that are NOT resident.
        let spilled = src.with(|j| j.force_cold_spill_all()).await;
        assert_eq!(spilled, 3, "all three instances went cold");
        assert_eq!(src.with(|j| j.cold_count()).await, 3);
        for key in &cold_keys {
            let key = *key;
            assert!(
                src.with(move |j| j.instance(key).is_none()).await,
                "precondition: instance {key} is off-heap"
            );
        }

        // Build the snapshot (streams the cold set from the store, bounded memory).
        let mut src_sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(src.clone(), 0).expect("snapshot dir"));
        let mut builder = src_sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.expect("build snapshot");

        // Ship it through the real chunked receive path into a fresh replica.
        let mut reader = snap.snapshot;
        let mut bytes = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            reader.read_to_end(&mut bytes).await.expect("read body");
        }
        let dst = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let mut dst_sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(dst.clone(), 0).expect("snapshot dir"));
        let mut received = dst_sm
            .begin_receiving_snapshot()
            .await
            .expect("begin receiving");
        {
            use tokio::io::AsyncWriteExt;
            received.write_all(&bytes).await.expect("write body");
            received.flush().await.expect("flush body");
        }
        dst_sm
            .install_snapshot(&snap.meta, received)
            .await
            .expect("install snapshot");

        // Every cold instance recovered onto the fresh replica, with its payload.
        let dst_state = dst.with(|j| j.state().clone()).await;
        for key in &cold_keys {
            let inst = dst_state
                .instances
                .get(key)
                .unwrap_or_else(|| panic!("cold instance {key} recovered onto the replica"));
            assert!(
                matches!(inst.variables.get("data"), Some(Value::Str(_))),
                "cold instance {key} recovered with its variables"
            );
        }
    }

    /// Fail-closed Raft snapshot build (the #1331 companion to the journal's
    /// fail-closed rotation): when the off-heap fold fails, `get_snapshot_builder`
    /// captures `None` and `build_snapshot` must return a `StorageError` and
    /// publish NOTHING — no snapshot body, no durable current-snapshot pointer.
    /// Otherwise openraft would purge the covered log on a successful build,
    /// permanently discarding the only copy of the omitted cold/spilled state.
    #[tokio::test]
    async fn build_snapshot_fails_closed_when_the_off_heap_fold_failed() {
        let src = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let src_sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(src.clone(), 0).expect("snapshot dir"));
        let snap_dir = src_sm.snapshot_dir.clone();

        // A builder whose capture is `None` — what `get_snapshot_builder` would
        // have produced had the streaming capture already been consumed.
        let mut builder = PartitionSnapshotBuilder {
            sm: src_sm.clone(),
            captured: None,
            last_applied: None,
            last_membership: StoredMembership::default(),
            test_writer_barrier: None,
            test_publish_barrier: None,
        };
        let err = match builder.build_snapshot().await {
            Ok(_) => panic!("an incomplete capture must fail the snapshot build"),
            Err(err) => err,
        };
        assert!(
            err.to_string().contains("already consumed"),
            "the error names the consumed capture, got: {err}"
        );

        // Nothing was published: no durable pointer, no snapshot body on disk.
        assert!(
            !snapshot_ptr_path(&snap_dir).is_file(),
            "no current-snapshot pointer may be written for a failed build"
        );
        assert!(
            std::fs::read_dir(&snap_dir)
                .map(|mut d| d.next().is_none())
                .unwrap_or(true),
            "no snapshot file may be left behind for a failed build"
        );

        // Control: a complete capture builds and publishes normally, so the
        // fail-closed guard does not break the happy path.
        let captured = src.with(|j| j.streaming_snapshot_parts()).await;
        let mut ok_builder = PartitionSnapshotBuilder {
            sm: src_sm.clone(),
            captured,
            last_applied: None,
            last_membership: StoredMembership::default(),
            test_writer_barrier: None,
            test_publish_barrier: None,
        };
        ok_builder
            .build_snapshot()
            .await
            .expect("a complete capture builds the snapshot");
        assert!(
            snapshot_ptr_path(&snap_dir).is_file(),
            "a complete capture publishes the durable current-snapshot pointer"
        );
    }

    fn unique_log_dir(tag: &str) -> std::path::PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        std::env::temp_dir().join(format!(
            "nanobpmn-raftlog-{}-{tag}-{nanos}",
            std::process::id()
        ))
    }

    /// A rejoining node whose log was compacted must restore engine state from the
    /// durable snapshot instead of deleting it. Regression for the rejoin-brick
    /// bug: boot used to unconditionally delete every on-disk snapshot and rebuild
    /// state by full log replay, so once the log was purged the deleted snapshot
    /// was the only source for the purged prefix and the partition failed to host
    /// (`expected [0, N), got [None, None)`). The durable current-snapshot pointer
    /// now survives the reboot, its applied metadata is adopted, and the engine is
    /// restored from it.
    #[tokio::test]
    async fn boot_restores_engine_from_the_durable_snapshot_instead_of_deleting_it() {
        use openraft::CommittedLeaderId;
        let snap_dir = unique_log_dir("snap-reboot").join("snapshots");

        // Boot 1: a state machine over an engine that has a deployed process.
        // Simulate an applied index (as openraft's `apply` would), then snapshot —
        // which writes the `.bin` AND the durable current-snapshot pointer.
        let applied = LogId::new(CommittedLeaderId::new(1, 0), 42);
        {
            let src = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
            src.with(|j| {
                let _ = j.apply_command_at(deploy_command(), 1).expect("deploy");
            })
            .await;
            let mut src_sm: Arc<PartitionStateMachine> = Arc::new(
                PartitionStateMachine::new(
                    src,
                    0,
                    snap_dir.clone(),
                    0,
                    false,
                    Arc::new(AtomicU64::new(u64::MAX)),
                )
                .expect("state machine"),
            );
            src_sm
                .inner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .last_applied = Some(applied);
            let mut builder = src_sm.get_snapshot_builder().await;
            let _ = builder.build_snapshot().await.expect("build snapshot");
        }

        // The durable pointer and the snapshot body it names are both on disk.
        assert!(
            snapshot_ptr_path(&snap_dir).is_file(),
            "the current-snapshot pointer is persisted"
        );

        // Boot 2: a BRAND-NEW, EMPTY engine + state machine reopens the same
        // snapshot dir (the rejoin). The old code would delete the snapshot here.
        let dst = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        assert!(
            dst.with(|j| j.state().processes.is_empty()).await,
            "the fresh engine starts empty"
        );
        let dst_sm: Arc<PartitionStateMachine> = Arc::new(
            PartitionStateMachine::new(
                dst.clone(),
                0,
                snap_dir.clone(),
                0,
                false,
                Arc::new(AtomicU64::new(u64::MAX)),
            )
            .expect("reopen state machine"),
        );

        // The snapshot was RETAINED (not deleted) and its applied metadata adopted.
        assert!(
            read_snapshot_ptr(&snap_dir).is_some(),
            "the snapshot survived the reboot"
        );
        let (last_applied, _) = dst_sm.clone().applied_state().await.expect("applied state");
        assert_eq!(
            last_applied,
            Some(applied),
            "boot adopts the snapshot's applied index (so openraft only replays the tail)"
        );

        // Restoring seeds the fresh engine with the snapshotted state.
        dst_sm
            .restore_from_current_snapshot()
            .await
            .expect("restore from snapshot");
        assert!(
            !dst.with(|j| j.state().processes.is_empty()).await,
            "the deployed process is recovered from the snapshot, not from a (purged) log"
        );
    }

    /// The purge-hole → snapshot-fallback boot detector (issue #111): a node that
    /// rejoins after being down longer than the leader's log-retention window has
    /// a committed index beyond what its local snapshot covers, and the log entries
    /// needed to replay that gap have been purged. Hosting from such a log trips
    /// openraft's defensive `LogIndexNotFound`, so the caller must host a fresh
    /// receiver instead. This pins the exact boundary: a hole is flagged only when
    /// `committed` is beyond the snapshot AND the reapply range is purged; a
    /// snapshot that already covers `committed`, a retained tail that still holds
    /// the range, and an empty/uncommitted dir are all NOT holes.
    #[tokio::test]
    async fn durable_log_purge_hole_detection_pins_the_boundary() {
        use openraft::CommittedLeaderId;
        use openraft::storage::RaftLogStorage;

        fn lid(index: u64) -> LogId<NodeId> {
            LogId::new(CommittedLeaderId::new(1, 0), index)
        }

        // Build a durable dir with the given committed/last_purged markers (via the
        // real log store, which persists `state.json`) and, when `snap_last` is
        // Some, a durable current-snapshot pointer covering that index.
        async fn setup(
            committed: Option<u64>,
            purged: Option<u64>,
            snap_last: Option<u64>,
        ) -> PathBuf {
            let dir = unique_log_dir("purge-hole");
            {
                let mut store = crate::raft_logstore::RaftLogStore::open(&dir).expect("open store");
                if let Some(c) = committed {
                    store
                        .save_committed(Some(lid(c)))
                        .await
                        .expect("save committed");
                }
                if let Some(p) = purged {
                    // `purge` persists `state.json` with BOTH markers, so committing
                    // first then purging leaves a durable (committed, last_purged).
                    store.purge(lid(p)).await.expect("purge");
                }
            }
            if let Some(s) = snap_last {
                let snap_dir = dir.join("snapshots");
                std::fs::create_dir_all(&snap_dir).expect("snap dir");
                let bin = snap_dir.join("snap-test.bin");
                std::fs::write(&bin, b"x").expect("snap body");
                let stored = StoredSnapshot {
                    meta: SnapshotMeta {
                        last_log_id: Some(lid(s)),
                        last_membership: StoredMembership::default(),
                        snapshot_id: "test-snap".to_string(),
                    },
                    path: bin,
                };
                write_snapshot_ptr(&snap_dir, &stored).expect("write snapshot ptr");
            }
            dir
        }

        // HOLE: committed=100 is beyond snapshot=10, and the reapply range (10,100]
        // is purged (last_purged=50). Hosting on-disk would trip LogIndexNotFound.
        let hole = setup(Some(100), Some(50), Some(10)).await;
        assert!(
            durable_log_has_purge_hole(&hole),
            "purged reapply range is a hole"
        );

        // HOLE: no snapshot at all, and the log is purged below the commit point.
        let no_snap = setup(Some(100), Some(50), None).await;
        assert!(
            durable_log_has_purge_hole(&no_snap),
            "no snapshot + purged log is a hole"
        );

        // NOT a hole: the snapshot already covers the committed point (reapply no-op).
        let covered = setup(Some(100), Some(50), Some(100)).await;
        assert!(
            !durable_log_has_purge_hole(&covered),
            "snapshot covers committed"
        );

        // NOT a hole: the retained tail still holds the reapply range
        // (last_purged=10 <= last_applied=10, so entries (10,100] are present).
        let retained = setup(Some(100), Some(10), Some(10)).await;
        assert!(
            !durable_log_has_purge_hole(&retained),
            "retained tail covers the range"
        );

        // NOT a hole: a fresh/empty dir with nothing committed hosts normally.
        let empty = setup(None, None, None).await;
        assert!(
            !durable_log_has_purge_hole(&empty),
            "no committed marker is not a hole"
        );
    }

    #[tokio::test]
    async fn durable_log_survives_restart_and_replays() {
        let log_dir = unique_log_dir("restart");

        // Boot 1: deploy a process through a crash-durable Raft log, then stop the
        // node (simulating a crash) — the engine state machine is volatile, so all
        // that persists is the durable log under `log_dir`.
        {
            let part = RaftPartition::bootstrap_single_durable(
                0,
                0,
                "http://self".into(),
                DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None),
                &log_dir,
            )
            .await
            .expect("bootstrap durable raft");

            let deploy_events = part.propose(deploy_command(), 1_000).await.expect("deploy");
            assert!(
                deploy_events
                    .iter()
                    .any(|e| matches!(e, Event::ProcessDeployed { .. })),
                "deploy applied on first boot (got {deploy_events:?})"
            );
            part.raft.shutdown().await.expect("clean shutdown");
        }

        // Boot 2: a brand-new, EMPTY engine + state machine reopens the same log
        // directory. If the durable log replays correctly, the previously deployed
        // process is known again — so creating an instance of it must succeed even
        // though nothing about the deploy lived in this process's memory.
        {
            let part = RaftPartition::bootstrap_single_durable(
                0,
                0,
                "http://self".into(),
                DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None),
                &log_dir,
            )
            .await
            .expect("recover durable raft");

            let create_events = part
                .propose(
                    Command::CreateInstance {
                        process_id: "p".into(),
                        variables: Default::default(),
                        tags: Vec::new(),
                        business_id: None,
                        process_definition_key: None,
                        version: None,
                    },
                    2_000,
                )
                .await
                .expect("create after recovery");
            assert!(
                create_events
                    .iter()
                    .any(|e| matches!(e, Event::ProcessInstanceCreated { .. })),
                "the deploy replayed from the durable log, so create succeeded \
                 after restart (got {create_events:?})"
            );
            part.raft.shutdown().await.expect("clean shutdown");
        }

        let _ = std::fs::remove_dir_all(&log_dir);
    }

    // ---- #1405: an environmental (off-heap read) failure during apply must fail
    // the replica CLOSED (StorageError → Raft core shutdown), never advance
    // `last_applied` past a command a healthy replica applied; and a DETERMINISTIC
    // rejection must NOT drop the independent later commands of the same batch. ----

    /// A service-task process whose instance parks on the `demo-work` job (the
    /// cold-spillable shape the #1388 journal tests use).
    fn wait_process_command() -> Command {
        let proc = ProcessBuilder::new("w")
            .start_event("s")
            .service_task("t", "demo-work")
            .end_event("e")
            .connect("s", "t")
            .connect("t", "e")
            .build()
            .expect("valid wait process");
        Command::DeployProcess(proc)
    }

    /// Drives `sm.apply` with one Normal entry carrying `commands` at `index`,
    /// returning the outcome. This is the exact function openraft calls when a
    /// committed entry reaches a replica.
    #[allow(
        clippy::result_large_err,
        reason = "test helper returning openraft's StorageError verbatim; the production apply signature is fixed by the RaftStateMachine trait"
    )]
    async fn apply_entry(
        sm: &Arc<PartitionStateMachine>,
        index: u64,
        commands: Vec<ReplicatedCommand>,
    ) -> Result<Vec<ReplicatedResponse>, StorageError<NodeId>> {
        use openraft::CommittedLeaderId;
        let entry = Entry::<RaftConfig> {
            log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
            payload: EntryPayload::Normal(ReplicatedBatch::new(commands)),
        };
        let mut sm = sm.clone();
        RaftStateMachine::apply(&mut sm, vec![entry]).await
    }

    /// Boots a single-partition state machine over a fresh engine backed by a
    /// cold-spill store at `store_path`, returning the engine handle + SM.
    async fn cold_backed_sm(
        store_path: &std::path::Path,
    ) -> (DeepthiHandle, Arc<PartitionStateMachine>) {
        let engine = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        engine
            .with({
                let store_path = store_path.to_path_buf();
                move |j| {
                    let store = Arc::new(
                        nano_server_storage::varspill::VarSpillStore::open(Some(&store_path))
                            .expect("open cold-spill store"),
                    );
                    j.set_cold_spill(store, 1, 0);
                }
            })
            .await;
        let sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(engine.clone(), 0).expect("sm"));
        (engine, sm)
    }

    /// Deploys `wait_process_command`, creates an instance, activates its
    /// `demo-work` job, then expires the activation lock (entry 4) so the instance
    /// goes idle and becomes cold-spillable. Returns `(job_key, instance_key)`.
    async fn setup_parked_job(
        sm: &Arc<PartitionStateMachine>,
        engine: &DeepthiHandle,
    ) -> (u64, u64) {
        let setup = [
            ReplicatedCommand {
                command: wait_process_command(),
                now: 1,
            },
            ReplicatedCommand {
                command: Command::create_instance("w"),
                now: 2,
            },
            ReplicatedCommand {
                command: Command::activate_jobs("demo-work", "w", 1, 60_000, 3),
                now: 3,
            },
            // Release the activation lock so the parked instance goes idle and is
            // cold-spillable (a locked job keeps its instance hot).
            ReplicatedCommand {
                command: Command::ExpireJobs { now: 64_000 },
                now: 64_000,
            },
        ];
        let mut job_key = None;
        for (i, cmd) in setup.into_iter().enumerate() {
            let outcome = apply_entry(sm, (i + 1) as u64, vec![cmd])
                .await
                .expect("setup entry applies");
            if i == 2 {
                job_key = outcome[0].items[0].events.iter().find_map(|e| match e {
                    Event::JobActivated { job_key, .. } => Some(*job_key),
                    _ => None,
                });
            }
        }
        let job_key = job_key.expect("activation produced a job");
        let instance_key = engine
            .with(|j| j.state().instances.keys().next().copied())
            .await
            .expect("one live instance");
        (job_key, instance_key)
    }

    /// Corrupts `instance_key`'s cold row in the store at `store_path` so any
    /// read of it fails environmentally (`EngineError::InstanceUnavailable`).
    fn corrupt_cold_row(store_path: &std::path::Path, instance_key: u64) {
        let conn = rusqlite::Connection::open(store_path).expect("open store");
        assert_eq!(
            conn.execute(
                "UPDATE cold SET snapshot = 'not-json{{' WHERE key = ?1",
                [instance_key as i64],
            )
            .expect("corrupt"),
            1
        );
    }

    /// The core defect (#1405): a batch whose LATER command hits an unreadable
    /// cold row on this replica must fail the WHOLE entry closed — `last_applied`
    /// must NOT advance and the failed command must journal nothing — so a healthy
    /// peer that applies the batch does not diverge from this replica.
    #[tokio::test]
    async fn apply_fails_closed_on_unreadable_state_instead_of_advancing_last_applied() {
        let store_dir = unique_log_dir("1405-failclosed");
        std::fs::create_dir_all(&store_dir).expect("store dir");
        let store_path = store_dir.join("replica.sqlite");
        let (engine, sm) = cold_backed_sm(&store_path).await;
        let (job_key, instance_key) = setup_parked_job(&sm, &engine).await;

        // Spill the instance cold, then corrupt its row so the CompleteJob read
        // fails environmentally on THIS replica.
        engine
            .with(|j| assert_eq!(j.force_cold_spill_all(), 1, "spilled cold"))
            .await;
        corrupt_cold_row(&store_path, instance_key);

        // A batch [deploy, complete_job]: the deploy succeeds, the completion hits
        // the unreadable row. The whole entry must abort closed at index 5.
        let batch = vec![
            ReplicatedCommand {
                command: deploy_command(),
                now: 5,
            },
            ReplicatedCommand {
                command: Command::complete_job(job_key),
                now: 6,
            },
        ];
        let refused = apply_entry(&sm, 5, batch).await;
        assert!(refused.is_err(), "the impaired replica fails closed");
        let (applied, _) = sm.clone().applied_state().await.expect("applied_state");
        assert_eq!(
            applied.map(|l| l.index),
            Some(4),
            "last_applied stays at the pre-batch index (4), never advanced past the \
             command a healthy replica applied"
        );

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Finding #2 (data loss): a batch mixing a DETERMINISTIC rejection with an
    /// independent later valid command must apply the valid command — the
    /// rejection is per-command, not a batch abort. `[CompleteJob(unknown),
    /// DeployProcess(valid)]` must still deploy.
    #[tokio::test]
    async fn deterministic_rejection_does_not_drop_later_independent_commands() {
        let store_dir = unique_log_dir("1405-detrej");
        std::fs::create_dir_all(&store_dir).expect("store dir");
        let store_path = store_dir.join("replica.sqlite");
        let (engine, sm) = cold_backed_sm(&store_path).await;

        // Entry 1: [CompleteJob(unknown key) -> deterministic JobNotFound,
        //           DeployProcess(valid) -> must still apply].
        let batch = vec![
            ReplicatedCommand {
                command: Command::complete_job(999_999),
                now: 1,
            },
            ReplicatedCommand {
                command: deploy_command(),
                now: 2,
            },
        ];
        let outcome = apply_entry(&sm, 1, batch)
            .await
            .expect("the entry applies (no environmental failure)");
        let items = &outcome[0].items;
        assert_eq!(items.len(), 2, "both commands get a per-command outcome");
        assert!(
            items[0].error.is_some(),
            "the unknown-job completion is a per-command rejection (got {:?})",
            items[0]
        );
        assert!(
            items[1].error.is_none() && !items[1].events.is_empty(),
            "the independent valid deploy is NOT dropped by the earlier rejection \
             (got {:?})",
            items[1]
        );
        // The deploy actually landed in replicated state.
        let procs = engine.with(|j| j.state().processes.len()).await;
        assert_eq!(
            procs, 1,
            "the valid deploy was applied, not silently dropped"
        );
        // And the entry was consumed (last_applied advanced) — deterministic
        // rejections keep replicas in lockstep.
        let (applied, _) = sm.clone().applied_state().await.expect("applied_state");
        assert_eq!(applied.map(|l| l.index), Some(1));

        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Spawns an engine over the durable SEGMENTED journal rooted at `dir`
    /// (replaying whatever it already holds).
    fn durable_engine(dir: &std::path::Path) -> DeepthiHandle {
        let (journal, _) = Journal::open_segmented(dir).expect("open segmented journal");
        DeepthiHandle::spawn(journal, 0, None)
    }

    /// Attaches the cold-spill store at `store_path` to `engine`.
    async fn attach_cold(engine: &DeepthiHandle, store_path: &std::path::Path) {
        let store_path = store_path.to_path_buf();
        engine
            .with(move |j| {
                let store = Arc::new(
                    nano_server_storage::varspill::VarSpillStore::open(Some(&store_path))
                        .expect("open cold-spill store"),
                );
                j.set_cold_spill(store, 1, 0);
            })
            .await;
    }

    /// Deterministically shuts a durable engine down: swaps its journal out of
    /// the actor and drops it HERE, which joins the journal's writer thread
    /// (`Journal::drop`). On return every acked write is on disk and no writer
    /// still holds the journal open — so a reopen can never race the old
    /// writer (the detached actor thread drops only the in-memory stand-in).
    async fn shutdown_engine(engine: DeepthiHandle) {
        let journal = engine
            .with(|j| std::mem::replace(j, Journal::in_memory_partition(0)))
            .await;
        drop(engine);
        drop(journal);
    }

    /// The commands of the entry the impaired replica fails closed on: a
    /// DETERMINISTIC rejection, a NON-IDEMPOTENT create, then the completion
    /// that hits the unreadable cold row.
    fn failing_entry(job_key: u64) -> Vec<ReplicatedCommand> {
        vec![
            ReplicatedCommand {
                command: Command::complete_job(999_999),
                now: 5,
            },
            ReplicatedCommand {
                command: Command::create_instance("w"),
                now: 6,
            },
            ReplicatedCommand {
                command: Command::complete_job(job_key),
                now: 7,
            },
        ]
    }

    /// Crash/restart proof (#1405): an impaired replica fails closed on a
    /// committed entry `[CompleteJob(unknown), CreateInstance, CompleteJob(
    /// unreadable)]` whose NON-IDEMPOTENT create already ran against live
    /// state. Entry-level atomicity means the journal saw NONE of it: reopening
    /// the real disk-backed journal shows zero instances from the entry and an
    /// applied index still at the previous entry. Re-applying after the store
    /// heals yields exactly one new instance, byte-identical to a clean replica
    /// — with the deterministic rejection ahead of the create in the same
    /// entry (the old per-command-ordinal drift shape).
    #[tokio::test]
    async fn failed_closed_entry_journals_nothing_and_reapplies_exactly_once() {
        let store_dir = unique_log_dir("1405-restart");
        std::fs::create_dir_all(&store_dir).expect("store dir");
        let store_path = store_dir.join("replica.sqlite");
        let journal_dir = store_dir.join("journal");

        // Phase 1: the impaired replica fails closed on entry 5.
        let (job_key, instance_key) = {
            let engine = durable_engine(&journal_dir);
            attach_cold(&engine, &store_path).await;
            let sm: Arc<PartitionStateMachine> =
                Arc::new(PartitionStateMachine::new_temp(engine.clone(), 0).expect("sm"));
            let (job_key, instance_key) = setup_parked_job(&sm, &engine).await;
            engine
                .with(|j| assert_eq!(j.force_cold_spill_all(), 1, "spilled cold"))
                .await;
            corrupt_cold_row(&store_path, instance_key);
            assert!(apply_entry(&sm, 5, failing_entry(job_key)).await.is_err());
            let (applied, _) = sm.clone().applied_state().await.expect("applied_state");
            assert_eq!(applied.map(|l| l.index), Some(4));
            drop(sm);
            shutdown_engine(engine).await;
            (job_key, instance_key)
        };

        // Phase 2: RESTART over the same durable journal. NOTHING of entry 5
        // is journaled: no instance from its create, applied index still 4.
        let engine2 = durable_engine(&journal_dir);
        let keys: Vec<u64> = engine2
            .with(|j| j.state().instances.keys().copied().collect())
            .await;
        assert_eq!(
            keys,
            vec![instance_key],
            "only the setup instance: the failed-closed entry's create was never journaled"
        );
        assert_eq!(
            engine2.with(|j| j.raft_applied().map(|(i, _)| i)).await,
            Some(4),
            "the journal's applied-entry record still names entry 4"
        );

        heal_cold_row(&store_path, instance_key);
        attach_cold(&engine2, &store_path).await;
        let sm2: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(engine2.clone(), 0).expect("sm"));
        sm2.seed_last_applied_from_journal()
            .await
            .expect("seed from journal");
        let (seeded, _) = sm2.clone().applied_state().await.expect("applied_state");
        assert_eq!(seeded.map(|l| l.index), Some(4), "seeded from the journal");

        let recovered = apply_entry(&sm2, 5, failing_entry(job_key))
            .await
            .expect("the healed replica re-applies the entry");
        let items = &recovered[0].items;
        assert!(
            items[0].error.is_some(),
            "the unknown-job rejection replicates"
        );
        assert!(
            items[1].error.is_none() && items[2].error.is_none(),
            "create + completion apply after recovery (got {items:?})"
        );
        let created = items
            .iter()
            .flat_map(|i| i.events.iter())
            .filter(|e| matches!(e, Event::ProcessInstanceCreated { .. }))
            .count();
        assert_eq!(created, 1, "the create executed exactly once");

        // Parity with a CLEAN replica that applied the same entry first time.
        let clean_dir = unique_log_dir("1405-restart-clean");
        std::fs::create_dir_all(&clean_dir).expect("clean store dir");
        let (clean_engine, clean_sm) = cold_backed_sm(&clean_dir.join("replica.sqlite")).await;
        let (clean_job, _) = setup_parked_job(&clean_sm, &clean_engine).await;
        assert_eq!(clean_job, job_key, "same key sequence on the clean replica");
        let clean = apply_entry(&clean_sm, 5, failing_entry(job_key))
            .await
            .expect("clean replica applies the entry");
        assert!(clean[0].items[1].error.is_none() && clean[0].items[2].error.is_none());

        let recovered_snapshot = engine2.with(|j| j.engine().snapshot()).await;
        let clean_snapshot = clean_engine.with(|j| j.engine().snapshot()).await;
        assert_eq!(
            recovered_snapshot.state, clean_snapshot.state,
            "recovered replica is byte-identical to the clean replica"
        );

        drop(sm2);
        shutdown_engine(engine2).await;
        let _ = std::fs::remove_dir_all(&store_dir);
        let _ = std::fs::remove_dir_all(&clean_dir);
    }

    /// Heals `instance_key`'s cold row at `store_path` with the bytes a
    /// healthy replica holds after [`setup_parked_job`].
    fn heal_cold_row(store_path: &std::path::Path, instance_key: u64) {
        let snap = {
            let mut e = nanobpmn_engine_core::Engine::with_partition(0);
            let _ = e.apply_command_at(wait_process_command(), 1).expect("d");
            let _ = e
                .apply_command_at(Command::create_instance("w"), 2)
                .expect("c");
            let _ = e
                .apply_command_at(Command::activate_jobs("demo-work", "w", 1, 60_000, 3), 3)
                .expect("a");
            let _ = e
                .apply_command_at(Command::ExpireJobs { now: 64_000 }, 64_000)
                .expect("x");
            e.snapshot_instance(instance_key)
                .expect("instance snapshot")
        };
        let store = nano_server_storage::varspill::VarSpillStore::open(Some(store_path))
            .expect("reopen store");
        store.put_cold(instance_key, &snap).expect("heal the row");
    }

    /// Fencing after an abort (#1405): once an entry fails closed, the live
    /// engine holds its partial, non-durable effects, so even after the cold
    /// read CLEARS a periodic checkpoint must not persist them. Abort → heal →
    /// checkpoint tick → restart: the journal-rebuilt replica holds no trace of
    /// the refused entry, so re-applying it creates exactly once.
    #[tokio::test]
    async fn aborted_entry_fences_checkpoints_until_rebuilt() {
        let store_dir = unique_log_dir("1405-fence");
        std::fs::create_dir_all(&store_dir).expect("store dir");
        let store_path = store_dir.join("replica.sqlite");
        let journal_dir = store_dir.join("journal");
        let job_key = {
            let engine = durable_engine(&journal_dir);
            attach_cold(&engine, &store_path).await;
            let sm: Arc<PartitionStateMachine> =
                Arc::new(PartitionStateMachine::new_temp(engine.clone(), 0).expect("sm"));
            let (job_key, instance_key) = setup_parked_job(&sm, &engine).await;
            engine
                .with(|j| assert_eq!(j.force_cold_spill_all(), 1, "spilled cold"))
                .await;
            corrupt_cold_row(&store_path, instance_key);
            assert!(apply_entry(&sm, 5, failing_entry(job_key)).await.is_err());
            // The cold read clears, then the periodic checkpoint ticks.
            heal_cold_row(&store_path, instance_key);
            assert!(engine.with(|j| j.is_fenced()).await, "fenced after abort");
            assert!(
                engine.with(|j| j.snapshot_and_rotate()).await.is_none(),
                "a checkpoint of the aborted prefix is refused"
            );
            assert!(
                engine
                    .with(|j| j.snapshot_and_rotate_lean())
                    .await
                    .is_none()
            );
            assert!(engine.with(|j| j.try_engine_snapshot()).await.is_none());
            // The streaming capture paths (production non-lean checkpoint loop
            // and the Raft snapshot builder) are fenced too.
            assert!(
                engine
                    .with(|j| j.streaming_snapshot_and_rotate())
                    .await
                    .is_none(),
                "a streaming checkpoint of the aborted prefix is refused"
            );
            assert!(
                engine
                    .with(|j| j.streaming_snapshot_parts())
                    .await
                    .is_none(),
                "a Raft snapshot capture of the aborted prefix is refused"
            );
            let mut builder = sm.clone().get_snapshot_builder().await;
            assert!(
                builder.build_snapshot().await.is_err(),
                "the Raft snapshot builder fails closed while fenced"
            );
            // Leader-local soft activation bypasses `apply_command_at`; it too
            // must hand out no work from the aborted prefix.
            assert!(
                engine
                    .with(|j| j.select_activatable_job_keys("demo-work", 10, u64::MAX / 2, false))
                    .await
                    .is_empty()
            );
            assert!(
                engine
                    .with(|j| j.activate_jobs_with_fetch("demo-work", "x", 10, 1, 1, Vec::new()))
                    .await
                    .is_empty()
            );
            assert!(
                engine
                    .with(move |j| j.resident_activated_job(job_key))
                    .await
                    .is_none()
            );
            assert!(
                engine
                    .with(|j| j.apply_command_at(Command::create_instance("w"), 9))
                    .await
                    .is_err_and(|e| e.is_environmental()),
                "no further mutation while fenced"
            );
            drop(sm);
            shutdown_engine(engine).await;
            job_key
        };
        let engine = durable_engine(&journal_dir);
        assert_eq!(
            engine.with(|j| j.state().instances.len()).await,
            1,
            "only the setup instance: nothing of the refused entry was checkpointed"
        );
        attach_cold(&engine, &store_path).await;
        let sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(engine.clone(), 0).expect("sm"));
        sm.seed_last_applied_from_journal().await.expect("seed");
        let out = apply_entry(&sm, 5, failing_entry(job_key))
            .await
            .expect("re-apply after rebuild");
        let created = out[0]
            .items
            .iter()
            .flat_map(|i| i.events.iter())
            .filter(|e| matches!(e, Event::ProcessInstanceCreated { .. }))
            .count();
        assert_eq!(created, 1);
        assert_eq!(
            engine.with(|j| j.state().instances.len()).await,
            2,
            "no duplicate"
        );
        drop(sm);
        shutdown_engine(engine).await;
        let _ = std::fs::remove_dir_all(&store_dir);
    }

    /// Boot-state selection (#1405): a durable journal AHEAD of the Raft
    /// snapshot must win. Restoring the older snapshot over the journal-rebuilt
    /// engine would discard the journaled suffix and make openraft re-execute
    /// it — a duplicate `ProcessInstanceCreated` and shifted key counters.
    #[tokio::test]
    async fn boot_prefers_a_journal_ahead_of_an_older_snapshot() {
        let dir = unique_log_dir("1405-boot-newest");
        let journal_dir = dir.join("journal");
        let snap_dir = dir.join("snapshots");
        let new_sm = |engine: &DeepthiHandle| -> Arc<PartitionStateMachine> {
            Arc::new(
                PartitionStateMachine::new(
                    engine.clone(),
                    0,
                    snap_dir.clone(),
                    0,
                    false,
                    Arc::new(AtomicU64::new(u64::MAX)),
                )
                .expect("sm"),
            )
        };
        let one = |command: Command, now: u64| vec![ReplicatedCommand { command, now }];
        let before = {
            let engine = durable_engine(&journal_dir);
            let mut sm = new_sm(&engine);
            apply_entry(&sm, 1, one(wait_process_command(), 1))
                .await
                .expect("1");
            apply_entry(&sm, 2, one(Command::create_instance("w"), 2))
                .await
                .expect("2");
            let mut builder = sm.get_snapshot_builder().await;
            let _ = builder.build_snapshot().await.expect("snapshot at 2");
            apply_entry(&sm, 3, one(Command::create_instance("w"), 3))
                .await
                .expect("3");
            let state = engine.with(|j| j.engine().snapshot().state).await;
            drop(sm);
            shutdown_engine(engine).await;
            state
        };
        let engine = durable_engine(&journal_dir);
        let sm = new_sm(&engine);
        let (adopted, _) = sm.clone().applied_state().await.expect("applied");
        assert_eq!(adopted.map(|l| l.index), Some(2), "the snapshot is older");
        sm.restore_newest_boot_state().await.expect("boot state");
        let (applied, _) = sm.clone().applied_state().await.expect("applied");
        assert_eq!(
            applied.map(|l| l.index),
            Some(3),
            "the journal's index wins"
        );
        let after = engine.with(|j| j.engine().snapshot().state).await;
        assert_eq!(
            after, before,
            "journal state kept: no suffix lost, no counter shift"
        );
        drop(sm);
        shutdown_engine(engine).await;

        // The converse: a snapshot strictly AHEAD of the journal is restored.
        let fresh = unique_log_dir("1405-boot-snapshot-ahead").join("journal");
        let engine = durable_engine(&fresh);
        let sm = new_sm(&engine);
        sm.restore_newest_boot_state().await.expect("boot state");
        assert_eq!(engine.with(|j| j.state().instances.len()).await, 1);
        let (applied, _) = sm.clone().applied_state().await.expect("applied");
        assert_eq!(applied.map(|l| l.index), Some(2), "the snapshot's index");
        drop(sm);
        shutdown_engine(engine).await;
        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(fresh.parent().expect("parent"));
    }

    /// Adopting a NEWER Raft snapshot over an older journal must make that
    /// snapshot the journal's recovery base (#1405). Otherwise, once a later
    /// entry is journaled (with a newer applied-entry record), the next restart
    /// prefers the journal — which never held the snapshot's state — and loses
    /// everything only the snapshot carried. Restart must yield snapshot +
    /// suffix, identical to a peer that applied every entry.
    #[tokio::test]
    async fn adopted_snapshot_becomes_the_journal_recovery_base() {
        let dir = unique_log_dir("1405-adopt-base");
        let snap_dir = dir.join("snapshots");
        let new_sm = |engine: &DeepthiHandle, snaps: &std::path::Path| {
            Arc::new(
                PartitionStateMachine::new(
                    engine.clone(),
                    0,
                    snaps.to_path_buf(),
                    0,
                    false,
                    Arc::new(AtomicU64::new(u64::MAX)),
                )
                .expect("sm"),
            )
        };
        let one = |command: Command, now: u64| vec![ReplicatedCommand { command, now }];
        // The reference peer: entries 1..=3, a snapshot at 3, then entry 4.
        let expected = {
            let engine = durable_engine(&dir.join("peer"));
            let mut sm = new_sm(&engine, &snap_dir);
            apply_entry(&sm, 1, one(wait_process_command(), 1))
                .await
                .expect("1");
            for i in 2..=3 {
                apply_entry(&sm, i, one(Command::create_instance("w"), i))
                    .await
                    .expect("create");
            }
            let mut builder = sm.get_snapshot_builder().await;
            let _ = builder.build_snapshot().await.expect("snapshot at 3");
            apply_entry(&sm, 4, one(Command::create_instance("w"), 4))
                .await
                .expect("4");
            let state = engine.with(|j| j.engine().snapshot().state).await;
            drop(sm);
            shutdown_engine(engine).await;
            state
        };
        // A replica whose journal only reached entry 1 (its own snapshots
        // elsewhere), then boots over the shared NEWER snapshot at 3.
        let journal_dir = dir.join("replica");
        {
            let engine = durable_engine(&journal_dir);
            let sm = new_sm(&engine, &dir.join("own-snapshots"));
            apply_entry(&sm, 1, one(wait_process_command(), 1))
                .await
                .expect("1");
            drop(sm);
            shutdown_engine(engine).await;
        }
        {
            let engine = durable_engine(&journal_dir);
            let sm = new_sm(&engine, &snap_dir);
            sm.restore_newest_boot_state()
                .await
                .expect("adopt snapshot");
            let (applied, _) = sm.clone().applied_state().await.expect("applied");
            assert_eq!(applied.map(|l| l.index), Some(3), "snapshot adopted");
            apply_entry(&sm, 4, one(Command::create_instance("w"), 4))
                .await
                .expect("4");
            drop(sm);
            shutdown_engine(engine).await;
        }
        // Restart: the journal (now at 4) wins and must hold the snapshot base.
        let engine = durable_engine(&journal_dir);
        let sm = new_sm(&engine, &snap_dir);
        sm.restore_newest_boot_state().await.expect("boot state");
        let (applied, _) = sm.clone().applied_state().await.expect("applied");
        assert_eq!(applied.map(|l| l.index), Some(4), "the journal wins");
        let after = engine.with(|j| j.engine().snapshot().state).await;
        assert_eq!(after, expected, "snapshot + suffix, nothing lost");
        drop(sm);
        shutdown_engine(engine).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A RUNTIME snapshot install (openraft's `install_snapshot`, a lagging
    /// replica catching up) must make the installed state the journal's recovery
    /// base exactly like boot-time adoption (#1405) — both go through the one
    /// `adopt_engine_snapshot` routine. Otherwise: journal at entry 1, install a
    /// snapshot at 3, journal entry 4, restart -> the journal (now at 4) wins but
    /// only holds entries 1 and 4, silently losing every snapshot-only instance.
    #[tokio::test]
    async fn runtime_installed_snapshot_becomes_the_journal_recovery_base() {
        let dir = unique_log_dir("1405-install-base");
        let new_sm = |engine: &DeepthiHandle, snaps: &std::path::Path| {
            Arc::new(
                PartitionStateMachine::new(
                    engine.clone(),
                    0,
                    snaps.to_path_buf(),
                    0,
                    false,
                    Arc::new(AtomicU64::new(u64::MAX)),
                )
                .expect("sm"),
            )
        };
        let one = |command: Command, now: u64| vec![ReplicatedCommand { command, now }];
        // The reference peer: entries 1..=3, a snapshot at 3, then entry 4.
        let peer = durable_engine(&dir.join("peer"));
        let mut peer_sm = new_sm(&peer, &dir.join("peer-snapshots"));
        apply_entry(&peer_sm, 1, one(wait_process_command(), 1))
            .await
            .expect("1");
        for i in 2..=3 {
            apply_entry(&peer_sm, i, one(Command::create_instance("w"), i))
                .await
                .expect("create");
        }
        let mut builder = peer_sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.expect("snapshot at 3");
        let mut bytes = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            let mut reader = snap.snapshot;
            reader.read_to_end(&mut bytes).await.expect("read body");
        }
        apply_entry(&peer_sm, 4, one(Command::create_instance("w"), 4))
            .await
            .expect("4");
        let expected = peer.with(|j| j.engine().snapshot().state).await;
        drop(peer_sm);
        shutdown_engine(peer).await;

        // A replica whose journal reached only entry 1 installs the snapshot at
        // runtime, then journals entry 4.
        let journal_dir = dir.join("replica");
        let snaps = dir.join("replica-snapshots");
        {
            let engine = durable_engine(&journal_dir);
            let mut sm = new_sm(&engine, &snaps);
            apply_entry(&sm, 1, one(wait_process_command(), 1))
                .await
                .expect("1");
            let mut received = sm
                .begin_receiving_snapshot()
                .await
                .expect("begin receiving");
            {
                use tokio::io::AsyncWriteExt;
                received.write_all(&bytes).await.expect("write body");
                received.flush().await.expect("flush body");
            }
            sm.install_snapshot(&snap.meta, received)
                .await
                .expect("install snapshot");
            apply_entry(&sm, 4, one(Command::create_instance("w"), 4))
                .await
                .expect("4");
            drop(sm);
            shutdown_engine(engine).await;
        }
        // Crash-restart: the journal (at 4) wins and must hold the installed base.
        let engine = durable_engine(&journal_dir);
        let sm = new_sm(&engine, &snaps);
        sm.restore_newest_boot_state().await.expect("boot state");
        let (applied, _) = sm.clone().applied_state().await.expect("applied");
        assert_eq!(applied.map(|l| l.index), Some(4), "the journal wins");
        let after = engine.with(|j| j.engine().snapshot().state).await;
        assert_eq!(after, expected, "installed snapshot + suffix, nothing lost");
        drop(sm);
        shutdown_engine(engine).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A LEGACY snapshot body (pre-#1405: decodes with `raft_applied = None`)
    /// adopted at runtime and interrupted BEFORE the Raft snapshot pointer is
    /// published must still boot at the snapshot's applied index: the
    /// checkpointed base derives its applied record from the snapshot's Raft
    /// meta. Otherwise the restart keeps the adopted engine state but seeds
    /// `last_applied = None`, and openraft re-executes the committed creates.
    #[tokio::test]
    async fn legacy_snapshot_adoption_interrupted_before_pointer_keeps_applied_index() {
        let dir = unique_log_dir("1405-legacy-adopt");
        let new_sm = |engine: &DeepthiHandle, snaps: &std::path::Path| {
            Arc::new(
                PartitionStateMachine::new(
                    engine.clone(),
                    0,
                    snaps.to_path_buf(),
                    0,
                    false,
                    Arc::new(AtomicU64::new(u64::MAX)),
                )
                .expect("sm"),
            )
        };
        let one = |command: Command, now: u64| vec![ReplicatedCommand { command, now }];
        let peer = durable_engine(&dir.join("peer"));
        let mut peer_sm = new_sm(&peer, &dir.join("peer-snapshots"));
        apply_entry(&peer_sm, 1, one(wait_process_command(), 1))
            .await
            .expect("1");
        for i in 2..=3 {
            apply_entry(&peer_sm, i, one(Command::create_instance("w"), i))
                .await
                .expect("create");
        }
        let mut builder = peer_sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.expect("snapshot at 3");
        let mut bytes = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            let mut reader = snap.snapshot;
            reader.read_to_end(&mut bytes).await.expect("read body");
        }
        let expected = peer.with(|j| j.engine().snapshot().state).await;
        drop(peer_sm);
        shutdown_engine(peer).await;
        let mut legacy =
            nano_server_storage::snapstream::read_streaming_engine_snapshot_from(&bytes[..])
                .expect("decode body");
        legacy.raft_applied = None;

        let journal_dir = dir.join("replica");
        let snaps = dir.join("replica-snapshots");
        {
            let engine = durable_engine(&journal_dir);
            let sm = new_sm(&engine, &snaps);
            apply_entry(&sm, 1, one(wait_process_command(), 1))
                .await
                .expect("1");
            // Adopt + checkpoint, then "crash" before the pointer is published.
            sm.adopt_engine_snapshot(legacy, &snap.meta)
                .await
                .expect("adopt");
            drop(sm);
            shutdown_engine(engine).await;
        }
        let engine = durable_engine(&journal_dir);
        let sm = new_sm(&engine, &snaps);
        sm.restore_newest_boot_state().await.expect("boot state");
        let (applied, _) = sm.clone().applied_state().await.expect("applied");
        assert_eq!(
            applied.map(|l| l.index),
            Some(3),
            "the adopted base carries the snapshot's applied index"
        );
        let after = engine.with(|j| j.engine().snapshot().state).await;
        assert_eq!(after, expected, "adopted state survives the crash");
        drop(sm);
        shutdown_engine(engine).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #1405: snapshot adoption must hold the journal checkpoint permit from
    /// BEFORE the engine restore through the base publication. Otherwise a
    /// periodic checkpoint could capture the adopted state (overwriting its
    /// variables) before adoption publishes the rebase floor. Barrier: the test
    /// holds the permit; adoption must not touch the engine until it is freed.
    #[tokio::test]
    async fn adoption_holds_checkpoint_permit_across_restore_and_publication() {
        let dir = unique_log_dir("1405-adopt-permit");
        let new_sm = |engine: &DeepthiHandle, snaps: &std::path::Path| {
            Arc::new(
                PartitionStateMachine::new(
                    engine.clone(),
                    0,
                    snaps.to_path_buf(),
                    0,
                    false,
                    Arc::new(AtomicU64::new(u64::MAX)),
                )
                .expect("sm"),
            )
        };
        let one = |command: Command, now: u64| vec![ReplicatedCommand { command, now }];
        let peer = durable_engine(&dir.join("peer"));
        let mut peer_sm = new_sm(&peer, &dir.join("peer-snapshots"));
        apply_entry(&peer_sm, 1, one(wait_process_command(), 1))
            .await
            .expect("1");
        for i in 2..=3 {
            apply_entry(&peer_sm, i, one(Command::create_instance("w"), i))
                .await
                .expect("create");
        }
        let mut builder = peer_sm.get_snapshot_builder().await;
        let snap = builder.build_snapshot().await.expect("snapshot at 3");
        let mut bytes = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            let mut reader = snap.snapshot;
            reader.read_to_end(&mut bytes).await.expect("read body");
        }
        let expected = peer.with(|j| j.engine().snapshot().state).await;
        drop(peer_sm);
        shutdown_engine(peer).await;
        let captured =
            nano_server_storage::snapstream::read_streaming_engine_snapshot_from(&bytes[..])
                .expect("decode body");

        let engine = durable_engine(&dir.join("replica"));
        let sm = new_sm(&engine, &dir.join("replica-snapshots"));
        apply_entry(&sm, 1, one(wait_process_command(), 1))
            .await
            .expect("1");
        let before = engine.with(|j| j.engine().snapshot().state).await;
        let seg = engine
            .with(|j| j.seg_shared())
            .await
            .expect("segmented journal");
        let held = seg.try_checkpoint_permit().expect("gate free");
        let meta = snap.meta.clone();
        let adopting = tokio::spawn({
            let sm = Arc::clone(&sm);
            async move { sm.adopt_engine_snapshot(captured, &meta).await }
        });
        // Deterministic barrier: adoption signals just before it waits on the
        // permit. Had it restored the engine first, the state check below
        // would observe the adopted state.
        sm.adoption_permit_waiting.notified().await;
        assert!(!adopting.is_finished(), "adoption waits for the permit");
        assert_eq!(
            engine.with(|j| j.engine().snapshot().state).await,
            before,
            "the engine is not restored while another checkpoint holds the permit"
        );
        drop(held);
        adopting.await.expect("join").expect("adopt");
        assert_eq!(engine.with(|j| j.engine().snapshot().state).await, expected);
        assert!(
            seg.try_checkpoint_permit().is_some(),
            "the permit is released after publication"
        );
        drop(sm);
        shutdown_engine(engine).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The round-robin start-dispatch cursor is the one engine field not
    /// derived from events, so a replica seeding `last_applied` from its
    /// journal (skipping command re-execution) must restore it from the
    /// journaled `RaftEntryApplied` record (#1405). Otherwise the restarted
    /// replica would route the next message start to a different partition
    /// than a peer that never restarted.
    #[tokio::test]
    async fn restarted_replica_routes_round_robin_starts_like_its_peer() {
        let dir = unique_log_dir("1405-rr");
        let snaps = dir.join("snapshots");
        let new_sm = |engine: &DeepthiHandle| {
            Arc::new(
                PartitionStateMachine::new(
                    engine.clone(),
                    0,
                    snaps.clone(),
                    0,
                    false,
                    Arc::new(AtomicU64::new(u64::MAX)),
                )
                .expect("sm"),
            )
        };
        let boot = |path: std::path::PathBuf| async move {
            let engine = durable_engine(&path);
            engine.with(|j| j.set_num_partitions(4)).await;
            engine
        };
        let deploy = Command::DeployProcess(
            ProcessBuilder::new("m")
                .message_start_event("s", "go")
                .end_event("e")
                .connect("s", "e")
                .build()
                .expect("valid message-start process"),
        );
        let one = |command: Command, now: u64| vec![ReplicatedCommand { command, now }];
        let start = || Command::correlate_message("go", "");
        let journal_dir = dir.join("journal");
        // Both replicas apply entries 1..=3 (a deploy + two round-robin starts).
        let peer = boot(dir.join("peer")).await;
        let peer_sm = new_sm(&peer);
        let replica = boot(journal_dir.clone()).await;
        let replica_sm = new_sm(&replica);
        for sm in [&peer_sm, &replica_sm] {
            apply_entry(sm, 1, one(deploy.clone(), 1)).await.expect("1");
            apply_entry(sm, 2, one(start(), 2)).await.expect("2");
            apply_entry(sm, 3, one(start(), 3)).await.expect("3");
        }
        let rr = peer.with(|j| j.engine().start_dispatch_rr()).await;
        assert_eq!(rr, 2, "two round-robin starts advanced the cursor");
        // The replica restarts and seeds `last_applied` from its journal.
        drop(replica_sm);
        shutdown_engine(replica).await;
        let replica = boot(journal_dir).await;
        let replica_sm = new_sm(&replica);
        replica_sm
            .restore_newest_boot_state()
            .await
            .expect("boot state");
        let (applied, _) = replica_sm.clone().applied_state().await.expect("applied");
        assert_eq!(applied.map(|l| l.index), Some(3), "seeded from the journal");
        assert_eq!(
            replica.with(|j| j.engine().start_dispatch_rr()).await,
            rr,
            "cursor restored from the applied-entry record"
        );
        // The next start routes identically on both.
        apply_entry(&peer_sm, 4, one(start(), 4)).await.expect("4");
        apply_entry(&replica_sm, 4, one(start(), 4))
            .await
            .expect("4");
        assert_eq!(
            replica.with(|j| j.engine().snapshot()).await.state,
            peer.with(|j| j.engine().snapshot()).await.state,
        );
        assert_eq!(
            replica.with(|j| j.engine().start_dispatch_rr()).await,
            peer.with(|j| j.engine().start_dispatch_rr()).await,
        );
        // Audit (#1405): the key generator is the other scalar not stored as
        // state; it is derived from the replayed keys and must match too.
        assert_eq!(
            replica.with(|j| j.engine().snapshot().next_local).await,
            peer.with(|j| j.engine().snapshot().next_local).await,
        );
        drop(peer_sm);
        drop(replica_sm);
        shutdown_engine(peer).await;
        shutdown_engine(replica).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// A torn final frame drops the WHOLE entry (#1405): a crash mid-append of
    /// a two-command entry can never keep the first command without the
    /// second, nor the events without the applied-entry record.
    #[tokio::test]
    async fn torn_final_frame_drops_the_whole_entry() {
        let dir = unique_log_dir("1405-torn");
        let journal_dir = dir.join("journal");
        {
            let engine = durable_engine(&journal_dir);
            let sm: Arc<PartitionStateMachine> =
                Arc::new(PartitionStateMachine::new_temp(engine.clone(), 0).expect("sm"));
            apply_entry(
                &sm,
                1,
                vec![ReplicatedCommand {
                    command: wait_process_command(),
                    now: 1,
                }],
            )
            .await
            .expect("entry 1");
            let two_creates = (0..2)
                .map(|i| ReplicatedCommand {
                    command: Command::create_instance("w"),
                    now: 2 + i,
                })
                .collect();
            apply_entry(&sm, 2, two_creates).await.expect("entry 2");
            drop(sm);
            shutdown_engine(engine).await;
        }
        // Sanity: intact, the journal holds both creates of entry 2.
        {
            let engine = durable_engine(&journal_dir);
            assert_eq!(engine.with(|j| j.state().instances.len()).await, 2);
            shutdown_engine(engine).await;
        }
        // Tear the tail of entry 2's frame (a crash mid-append).
        let active = journal_dir.join(nano_server_storage::seglog::ACTIVE_NAME);
        let len = std::fs::metadata(&active).expect("active segment").len();
        std::fs::OpenOptions::new()
            .write(true)
            .open(&active)
            .expect("open active")
            .set_len(len - 3)
            .expect("truncate");

        let engine = durable_engine(&journal_dir);
        assert_eq!(
            engine.with(|j| j.state().processes.len()).await,
            1,
            "entry 1 survives"
        );
        assert_eq!(
            engine.with(|j| j.state().instances.len()).await,
            0,
            "NEITHER create of the torn entry survives — all or nothing"
        );
        assert_eq!(
            engine.with(|j| j.raft_applied().map(|(i, _)| i)).await,
            Some(1),
            "the applied-entry record of the torn entry is gone with it"
        );
        shutdown_engine(engine).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Bootstrap without a Raft snapshot must NOT re-apply entries the durable
    /// journal already holds (#1405): before the fix `last_applied` started at
    /// `None`, so openraft re-applied the whole committed log on top of the
    /// journal-rebuilt engine and every non-idempotent create ran twice.
    #[tokio::test]
    async fn bootstrap_without_snapshot_does_not_reapply_journaled_entries() {
        let dir = unique_log_dir("1405-bootstrap");
        let journal_dir = dir.join("journal");
        let log_dir = dir.join("raftlog");
        {
            let engine = durable_engine(&journal_dir);
            let part = RaftPartition::bootstrap_single_durable(
                0,
                0,
                "http://self".into(),
                engine.clone(),
                &log_dir,
            )
            .await
            .expect("bootstrap durable raft");
            part.propose(wait_process_command(), 1)
                .await
                .expect("deploy");
            part.propose(Command::create_instance("w"), 2)
                .await
                .expect("create");
            part.raft.shutdown().await.expect("clean shutdown");
            drop(part);
            shutdown_engine(engine).await;
        }
        assert!(
            read_snapshot_ptr(&log_dir.join("snapshots")).is_none(),
            "no Raft snapshot: the bootstrap path under test"
        );

        let engine = durable_engine(&journal_dir);
        assert_eq!(engine.with(|j| j.state().instances.len()).await, 1);
        let journaled = engine
            .with(|j| j.raft_applied().map(|(i, _)| i))
            .await
            .expect("the journal recorded its last applied entry");
        let part = RaftPartition::bootstrap_single_durable(
            0,
            0,
            "http://self".into(),
            engine.clone(),
            &log_dir,
        )
        .await
        .expect("recover durable raft");
        assert!(
            part.raft.metrics().borrow().last_applied.map(|l| l.index) >= Some(journaled),
            "last_applied seeded from the journal"
        );
        assert_eq!(
            engine.with(|j| j.state().instances.len()).await,
            1,
            "the journaled create was NOT re-applied on bootstrap"
        );
        part.raft.shutdown().await.expect("clean shutdown");
        drop(part);
        shutdown_engine(engine).await;
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The opaque replication state journaled in `RaftEntryApplied` decodes back
    /// to the exact log id the seed relies on (#1405).
    #[test]
    fn journaled_raft_state_round_trips() {
        use openraft::CommittedLeaderId;
        let log_id = LogId::new(CommittedLeaderId::new(3, 1), 17);
        let json = journaled_raft_state(log_id, &StoredMembership::default()).expect("encode");
        let back: JournaledRaftState = serde_json::from_str(&json).expect("decode");
        assert_eq!(back.log_id, log_id);
    }

    /// Polls `cond` until it holds or `timeout_ms` elapses.
    async fn wait_until(timeout_ms: u64, mut cond: impl FnMut() -> bool) -> bool {
        let deadline = std::time::Instant::now() + std::time::Duration::from_millis(timeout_ms);
        loop {
            if cond() {
                return true;
            }
            if std::time::Instant::now() >= deadline {
                return false;
            }
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
    }

    #[tokio::test]
    async fn three_voters_replicate_and_commit() {
        use crate::raft_net::LocalCluster;

        // An in-process transport routes RPCs straight into the peers' Raft
        // instances, so this is a genuine 3-voter group (real elections, real
        // AppendEntries, real quorum), just without the network bytes.
        let cluster = LocalCluster::default();
        let transport: Arc<dyn RaftTransport> = Arc::new(cluster.clone());

        let mut parts = Vec::new();
        for id in 0u64..3 {
            let p = RaftPartition::bootstrap_member(
                id,
                0,
                DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None),
                transport.clone(),
                None,
                false,
            )
            .await
            .expect("boot member");
            cluster.register(0, id, p.raft.clone());
            parts.push(p);
        }

        // Form the group once, then let node 0 win the initial election.
        let mut members = BTreeMap::new();
        for id in 0u64..3 {
            members.insert(id, BasicNode::new(format!("local-{id}")));
        }
        parts[0].initialize(members).await.expect("form group");
        assert!(
            wait_until(3_000, || parts[0].raft.metrics().borrow().current_leader
                == Some(0))
            .await,
            "node 0 should win the initial election"
        );

        // Propose on the leader: with RF=3 this commits only once a quorum (2 of
        // 3) has the entry, exercising the network end to end.
        let deploy_events = parts[0]
            .propose(deploy_command(), 1_000)
            .await
            .expect("deploy");
        assert!(
            deploy_events
                .iter()
                .any(|e| matches!(e, Event::ProcessDeployed { .. })),
            "the deploy committed via quorum and applied (got {deploy_events:?})"
        );

        let target = parts[0]
            .raft
            .metrics()
            .borrow()
            .last_applied
            .map(|l| l.index)
            .unwrap_or(0);
        assert!(target >= 1, "leader applied at least the deploy entry");

        // Every follower converges to the same applied index — proof the entry
        // replicated to and applied on all three voters.
        for (id, p) in parts.iter().enumerate() {
            let raft = &p.raft;
            let applied = wait_until(3_000, || {
                raft.metrics()
                    .borrow()
                    .last_applied
                    .map(|l| l.index)
                    .unwrap_or(0)
                    >= target
            })
            .await;
            assert!(applied, "node {id} did not apply up to index {target}");
        }

        for p in parts {
            p.raft.shutdown().await.expect("clean shutdown");
        }
    }

    /// A **follower replica** (`evict_eligible = true`, not currently leader) has
    /// no read-model exporter to reclaim terminal instances, so `apply` evicts
    /// the shell the moment an instance turns terminal — otherwise completed
    /// instances accumulate in a follower's hot state without bound (the RF>1
    /// leak). The **leader** keeps the shell resident (it serves reads/status
    /// from its engine, per ADR-0012).
    #[tokio::test]
    async fn follower_replica_evicts_terminal_instances() {
        use crate::raft_net::LocalCluster;

        let cluster = LocalCluster::default();
        let transport: Arc<dyn RaftTransport> = Arc::new(cluster.clone());

        // Three voters of partition 0. The leader (node 0) is not evict-eligible
        // (an owned partition defers to its exporter); the two followers are.
        let mut parts = Vec::new();
        let mut engines = Vec::new();
        for id in 0u64..3 {
            let engine = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
            engines.push(engine.clone());
            let evict_eligible = id != 0;
            let p = RaftPartition::bootstrap_member(
                id,
                0,
                engine,
                transport.clone(),
                None,
                evict_eligible,
            )
            .await
            .expect("boot member");
            cluster.register(0, id, p.raft.clone());
            parts.push(p);
        }

        let mut members = BTreeMap::new();
        for id in 0u64..3 {
            members.insert(id, BasicNode::new(format!("local-{id}")));
        }
        parts[0].initialize(members).await.expect("form group");
        assert!(
            wait_until(3_000, || parts[0].raft.metrics().borrow().current_leader
                == Some(0))
            .await,
            "node 0 wins the initial election"
        );

        // Deploy, then create an instance of the `p` process (start -> end, no
        // wait state) so it runs straight to a terminal ProcessInstanceCompleted
        // in the same replicated command on every voter.
        parts[0]
            .propose(deploy_command(), 1_000)
            .await
            .expect("deploy");
        parts[0]
            .propose(
                Command::CreateInstance {
                    process_id: "p".into(),
                    variables: Default::default(),
                    tags: Vec::new(),
                    business_id: None,
                    process_definition_key: None,
                    version: None,
                },
                2_000,
            )
            .await
            .expect("create");

        // The leader retains the terminal shell; both followers evict it (their
        // eviction is a fire-and-forget actor hop after apply, so poll to drain).
        let resident = |engine: DeepthiHandle| async move {
            engine.with(|j| j.engine().state().instances.len()).await
        };
        let mut followers_evicted = false;
        for _ in 0..300 {
            let f1 = resident(engines[1].clone()).await;
            let f2 = resident(engines[2].clone()).await;
            if f1 == 0 && f2 == 0 {
                followers_evicted = true;
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(
            followers_evicted,
            "both followers evict the terminal instance shell"
        );
        assert_eq!(
            resident(engines[0].clone()).await,
            1,
            "the leader keeps the terminal shell resident for its serving path"
        );

        for p in parts {
            p.raft.shutdown().await.expect("clean shutdown");
        }
    }

    #[test]
    fn clamp_log_range_rejects_degenerate_ranges() {
        use std::ops::Bound;
        // Normal half-open range -> inclusive equivalent.
        assert_eq!(clamp_log_range(&(5u64..9)), Some(5..=8));
        // Inclusive range passes through.
        assert_eq!(clamp_log_range(&(5u64..=8)), Some(5..=8));
        // Unbounded ends map to the full domain.
        assert_eq!(clamp_log_range(&(..)), Some(u64::MIN..=u64::MAX));
        assert_eq!(clamp_log_range(&(3u64..)), Some(3..=u64::MAX));
        // Single-element half-open range.
        assert_eq!(clamp_log_range(&(7u64..8)), Some(7..=7));
        // Empty half-open range (start == end) -> None, never panics.
        assert_eq!(clamp_log_range(&(5u64..5)), None);
        // Inverted range (the openraft rejoin/snapshot-race case) -> None.
        // Built from values (not literals) to mirror openraft's computed
        // `(last_applied, committed]` window.
        let (hi, lo) = (9u64, 5u64);
        assert_eq!(clamp_log_range(&(hi..lo)), None);
        assert_eq!(
            clamp_log_range(&(Bound::Included(9u64), Bound::Excluded(5u64))),
            None
        );
        // Exclusive end of 0 is empty, not an underflow panic.
        assert_eq!(clamp_log_range(&(0u64..0)), None);
    }

    /// Failure-injection regression (#1357): a poisoned `PartitionStateMachine`
    /// mutex — a panic while the `inner` / `current_snapshot` guard is held
    /// mid-mutation — must not wedge the partition. Every acquisition recovers the
    /// guard (`unwrap_or_else(|e| e.into_inner())`) instead of panicking on the
    /// consensus hot path. Poison each mutex separately, assert the poisoning
    /// actually took, then drive the applied-state read and snapshot
    /// publication/retrieval paths across both recovered mutexes, verifying the
    /// applied log id and membership match the snapshot metadata — including
    /// after a fresh state machine restores from the durable pointer on disk.
    /// With the previous panicking acquisitions each exercise below would fail
    /// with a `PoisonError`; with the recovery behaviour they pass.
    #[tokio::test]
    async fn state_machine_survives_poisoned_inner_and_snapshot_locks() {
        use openraft::CommittedLeaderId;

        let lid = |index: u64| LogId::new(CommittedLeaderId::new(1, 0), index);
        let engine = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        engine
            .with(|j| {
                let _ = j.apply_command_at(deploy_command(), 1).expect("deploy");
            })
            .await;
        let sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(engine.clone(), 0).expect("snapshot dir"));
        let snap_dir = sm.snapshot_dir.clone();

        // Seed a membership so the applied-state tuple covers both fields.
        let membership = StoredMembership::new(
            Some(lid(2)),
            openraft::Membership::new(vec![BTreeSet::from([0u64])], None),
        );
        sm.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last_membership = membership.clone();

        // Poison `inner` (a panic while the guard is held) and prove it took.
        {
            let inner: &Mutex<SmMeta> = &sm.inner;
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _g = inner.lock();
                    panic!("poison state-machine meta");
                }))
                .is_err(),
                "the injected panic must fire while the meta guard is held"
            );
            assert!(
                inner.lock().is_err(),
                "the meta mutex must be poisoned by the injected panic"
            );
        }

        // Applied-state reads recover the poisoned guard and serve the metadata.
        let (last_applied, last_membership) =
            sm.clone().applied_state().await.expect("applied_state");
        assert_eq!(last_applied, None);
        assert_eq!(last_membership, membership);

        // Poison `current_snapshot` too, and prove it took.
        {
            let current: &Mutex<Option<StoredSnapshot>> = &sm.current_snapshot;
            assert!(
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    let _g = current.lock();
                    panic!("poison current snapshot");
                }))
                .is_err(),
                "the injected panic must fire while the snapshot guard is held"
            );
            assert!(
                current.lock().is_err(),
                "the snapshot mutex must be poisoned by the injected panic"
            );
        }

        // Snapshot publication writes through BOTH recovered mutexes: the build
        // captures `inner` (applied metadata) and publishes into
        // `current_snapshot`.
        let applied = lid(42);
        sm.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .last_applied = Some(applied);
        let mut builder = sm.clone().get_snapshot_builder().await;
        let snap = builder
            .build_snapshot()
            .await
            .expect("snapshot build recovers the poisoned snapshot mutex");
        assert_eq!(snap.meta.last_log_id, Some(applied));
        assert_eq!(snap.meta.last_membership, membership);

        // Snapshot retrieval recovers the same poisoned mutex and serves the
        // publication, with metadata matching the applied state.
        let current = sm
            .clone()
            .get_current_snapshot()
            .await
            .expect("get_current_snapshot")
            .expect("a snapshot was published");
        assert_eq!(current.meta, snap.meta);
        let (last_applied, last_membership) =
            sm.clone().applied_state().await.expect("applied_state");
        assert_eq!(last_applied, Some(applied));
        assert_eq!(last_membership, snap.meta.last_membership);

        // After restoration from disk (a fresh state machine over the same
        // snapshot dir, i.e. the reboot path), the applied log id and membership
        // still match the snapshot metadata.
        let restored: Arc<PartitionStateMachine> = Arc::new(
            PartitionStateMachine::new(
                engine,
                0,
                snap_dir,
                0,
                false,
                Arc::new(AtomicU64::new(u64::MAX)),
            )
            .expect("reopen state machine"),
        );
        let (restored_applied, restored_membership) = restored
            .clone()
            .applied_state()
            .await
            .expect("applied_state");
        assert_eq!(restored_applied, Some(applied));
        assert_eq!(restored_membership, snap.meta.last_membership);
    }

    #[tokio::test]
    async fn try_get_log_entries_never_panics_on_inverted_range() {
        // Regression for the node-rejoin crash: openraft can request an
        // inverted `(last_applied, committed]` window during a promoted
        // group's snapshot-send race. `BTreeMap::range` would panic and, under
        // the fatal-panic build, abort the whole node. The guard must instead
        // return no entries.
        let mut store = MemLogStore::default();
        // Values, not literals, so this mirrors openraft's computed window
        // (and isn't a compile-time reversed-range lint).
        let (hi, lo) = (9u64, 5u64);
        let inverted = store
            .try_get_log_entries(hi..lo)
            .await
            .expect("inverted range returns Ok, not a panic");
        assert!(inverted.is_empty());
        let same = 5u64;
        let empty = store
            .try_get_log_entries(same..same)
            .await
            .expect("empty range returns Ok");
        assert!(empty.is_empty());
    }

    /// #1473 (defect 2): a snapshot build that is STILL IN FLIGHT (parked on the
    /// build semaphore) when its member is retired must never publish into the
    /// snapshot dir the successor reopens. Barrier-driven, not sleep-based.
    #[tokio::test]
    async fn a_build_in_flight_during_retire_never_publishes() {
        let engine = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        engine
            .with(|j| {
                let _ = j.apply_command_at(deploy_command(), 1).expect("deploy");
            })
            .await;
        let sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(engine.clone(), 0).expect("snapshot dir"));
        let snap_dir = sm.snapshot_dir.clone();

        // Hold every build permit so the build parks after its capture.
        let permits = snapshot_build_sem()
            .acquire_many(snapshot_build_concurrency().max(1))
            .await
            .expect("hold every build permit");
        let mut builder = Arc::clone(&sm).get_snapshot_builder().await;
        assert!(builder.captured.is_some(), "a live member captures");
        let build = tokio::spawn(async move { builder.build_snapshot().await });

        // Retire: the parked build holds no lifecycle guard, so the exclusive
        // lock is granted at once and the member's epoch ends.
        drop(engine.lifecycle_exclusive().await);

        // Release the build: it re-enters the lifecycle lock, is refused, and
        // fails before any file I/O.
        drop(permits);
        let result = build.await.expect("the build task ran");
        assert!(
            result.is_err(),
            "a build resumed after retirement must fail"
        );
        assert!(
            !snapshot_ptr_path(&snap_dir).is_file(),
            "a retired build must never write the durable snapshot pointer"
        );
        let leftovers: Vec<_> = std::fs::read_dir(&snap_dir)
            .map(|d| d.flatten().map(|e| e.file_name()).collect())
            .unwrap_or_default();
        assert!(
            leftovers.is_empty(),
            "a retired build writes no artifact, found {leftovers:?}"
        );
        let _ = std::fs::remove_dir_all(&snap_dir);
    }

    /// #1473 (defect 3): retiring a volatile member reclaims its temp snapshot
    /// staging dir; a durable member's snapshot dir is never touched.
    #[tokio::test]
    async fn retire_reclaims_a_volatile_snapshot_dir_but_never_a_durable_one() {
        // Volatile member: temp-anchored snapshot dir.
        let volatile_engine = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let volatile = RaftPartition::bootstrap_single(0, 0, "127.0.0.1:0".into(), volatile_engine)
            .await
            .expect("boot a volatile single voter");
        assert!(volatile.has_volatile_snapshot_dir());
        let volatile_dir = volatile.snapshot_dir().to_path_buf();
        assert!(volatile_dir.is_dir(), "the volatile staging dir exists");
        volatile.retire().await.expect("retire the volatile member");
        assert!(volatile.is_retired());
        assert!(
            !volatile_dir.exists(),
            "retiring a volatile member reclaims its staging dir"
        );

        // Durable member: snapshot dir anchors under the log dir.
        let log_dir = unique_log_dir("retire-durable");
        let durable_engine = DeepthiHandle::spawn(Journal::in_memory_partition(1), 1, None);
        let durable = RaftPartition::bootstrap_single_durable(
            1,
            1,
            "127.0.0.1:0".into(),
            durable_engine,
            &log_dir,
        )
        .await
        .expect("boot a durable single voter");
        assert!(!durable.has_volatile_snapshot_dir());
        let durable_snap_dir = durable.snapshot_dir().to_path_buf();
        durable.retire().await.expect("retire the durable member");
        assert!(durable.is_retired());
        assert!(
            durable_snap_dir.exists(),
            "a durable snapshot dir is a recovery point — never reclaimed on retire"
        );
        let _ = std::fs::remove_dir_all(&log_dir);
    }

    /// #1473 (defect 3, bound): N rebuilds leave at most one live volatile
    /// snapshot directory — each retire reclaims the predecessor's staging dir.
    #[tokio::test]
    async fn repeated_rebuilds_leave_at_most_one_volatile_snapshot_dir() {
        fn live_volatile_dirs(partition_id: u64) -> usize {
            let prefix = format!(
                "{RAFT_SNAPSHOT_DIR_PREFIX}{}-p{partition_id}-",
                std::process::id()
            );
            std::fs::read_dir(std::env::temp_dir())
                .map(|entries| {
                    entries
                        .flatten()
                        .filter(|e| {
                            e.file_name()
                                .to_str()
                                .is_some_and(|n| n.starts_with(&prefix))
                                && e.file_type().map(|t| t.is_dir()).unwrap_or(false)
                        })
                        .count()
                })
                .unwrap_or(0)
        }

        let partition_id = 7;
        let engine = DeepthiHandle::spawn(
            Journal::in_memory_partition(partition_id),
            partition_id,
            None,
        );
        // Boot + retire the same member repeatedly (each rebuild allocates a
        // fresh temp staging dir). After every retire the predecessor's dir is
        // reclaimed, so at most the CURRENT member's dir survives.
        let mut member =
            RaftPartition::bootstrap_single(0, partition_id, "127.0.0.1:0".into(), engine.clone())
                .await
                .expect("boot the initial volatile member");
        for round in 0..4 {
            member.retire().await.expect("retire the member");
            assert!(
                live_volatile_dirs(partition_id) <= 1,
                "after retire #{round} at most one volatile dir may survive, found {}",
                live_volatile_dirs(partition_id)
            );
            member = RaftPartition::bootstrap_single(
                0,
                partition_id,
                "127.0.0.1:0".into(),
                engine.clone(),
            )
            .await
            .expect("boot the successor volatile member");
        }
        member.retire().await.expect("retire the last member");
        assert_eq!(
            live_volatile_dirs(partition_id),
            0,
            "retiring the last member reclaims its staging dir too"
        );
    }

    /// #1473 DEFECT-CLASS GUARD: every engine-touching state-machine operation
    /// (apply, snapshot capture/build, receive, install) that runs or resumes
    /// after its member was retired is refused BEFORE it touches the engine or
    /// the snapshot dir — including an install whose receive began before the
    /// retirement (the "passed the fence, then parked" interleaving). One lock,
    /// one epoch: no operation needs a bespoke fence.
    #[tokio::test]
    async fn every_state_machine_operation_is_refused_after_retirement() {
        // A source with a deployed definition, to produce a real snapshot.
        let src = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        src.with(|j| {
            let _ = j.apply_command_at(deploy_command(), 1).expect("deploy");
        })
        .await;
        let src_sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(src, 0).expect("src dir"));
        let mut src_builder = Arc::clone(&src_sm).get_snapshot_builder().await;
        let snap = src_builder.build_snapshot().await.expect("source snapshot");
        let mut bytes = Vec::new();
        {
            use tokio::io::AsyncReadExt;
            let mut reader = snap.snapshot;
            reader.read_to_end(&mut bytes).await.expect("read snapshot");
        }

        // The member under test: begin receiving BEFORE retirement.
        let dst = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let mut sm: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(dst.clone(), 0).expect("dst dir"));
        let mut received = sm.begin_receiving_snapshot().await.expect("begin receive");
        {
            use tokio::io::AsyncWriteExt;
            received.write_all(&bytes).await.expect("write body");
            received.flush().await.expect("flush body");
        }

        // Retire the member (the exclusive lifecycle lock ends its epoch).
        drop(dst.lifecycle_exclusive().await);

        // install: refused before deserialization/adoption — engine untouched.
        let install = sm.install_snapshot(&snap.meta, received).await;
        assert!(install.is_err(), "a retired member must refuse an install");
        let processes = dst.with(|j| j.state().processes.len()).await;
        assert_eq!(
            processes, 0,
            "a refused install never adopts into the engine"
        );
        assert!(!snapshot_ptr_path(&sm.snapshot_dir).is_file());

        // receive: refused.
        assert!(sm.begin_receiving_snapshot().await.is_err());

        // apply: refused, engine untouched.
        let applied = apply_entry(
            &sm,
            1,
            vec![ReplicatedCommand {
                command: deploy_command(),
                now: 1,
            }],
        )
        .await;
        assert!(applied.is_err(), "a retired member must refuse an apply");
        let processes = dst.with(|j| j.state().processes.len()).await;
        assert_eq!(processes, 0, "a refused apply never reaches the engine");

        // capture/build: no capture, so the build fails before file I/O.
        let mut builder = Arc::clone(&sm).get_snapshot_builder().await;
        assert!(
            builder.captured.is_none(),
            "a retired member captures nothing"
        );
        assert!(builder.build_snapshot().await.is_err());
        assert!(!snapshot_ptr_path(&sm.snapshot_dir).is_file());

        // A SUCCESSOR bound to the new epoch on the same engine is admitted.
        let successor: Arc<PartitionStateMachine> =
            Arc::new(PartitionStateMachine::new_temp(dst.clone(), 0).expect("successor dir"));
        apply_entry(
            &successor,
            1,
            vec![ReplicatedCommand {
                command: deploy_command(),
                now: 1,
            }],
        )
        .await
        .expect("the successor applies");
        for dir in [
            &src_sm.snapshot_dir,
            &sm.snapshot_dir,
            &successor.snapshot_dir,
        ] {
            let _ = std::fs::remove_dir_all(dir);
        }
    }

    /// #1473 (review): a member that REOPENS a durable snapshot dir continues in
    /// a strictly newer generation, so a late predecessor build/install can never
    /// collide with (or be mistaken for) the successor's artifact on the shared
    /// dir. The generation is derived from the durable pointer's file name.
    #[test]
    fn a_reopened_snapshot_dir_continues_in_a_newer_generation() {
        assert_eq!(snapshot_generation_of("snap-1.bin"), 0);
        assert_eq!(snapshot_generation_of("snap-g1-1.bin"), 1);
        assert_eq!(snapshot_generation_of("snap-installed-g3-2.bin"), 2 + 1);
        assert_eq!(snapshot_generation_of("snap-g41-7.bin"), 41);
        // An unrecognized shape is generation 0 (pre-#1473), never a panic.
        assert_eq!(snapshot_generation_of("unrelated.bin"), 0);
    }

    /// #1473 DEFECT-CLASS GUARD (drain): retirement waits for every in-flight
    /// shared holder of the engine lifecycle lock (a parked apply / build /
    /// install) and fails CLOSED past its bound, so a rebuild never proceeds
    /// over predecessor work still touching the engine. Once the holder leaves,
    /// retirement succeeds.
    #[tokio::test]
    async fn retire_drains_in_flight_work_and_fails_closed_past_the_bound() {
        let engine = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let member = RaftPartition::bootstrap_single(0, 0, "127.0.0.1:0".into(), engine.clone())
            .await
            .expect("boot a volatile single voter");
        // Simulate a parked apply/install: hold the member's epoch shared.
        let in_flight = engine
            .lifecycle_enter(engine.lifecycle_epoch())
            .await
            .expect("the live member's epoch is admitted");
        let blocked = member
            .retire_within(std::time::Duration::from_millis(50))
            .await;
        assert!(
            blocked.is_err(),
            "retire must fail closed while work is in flight"
        );
        drop(in_flight);
        let exclusive = member
            .retire_within(std::time::Duration::from_secs(5))
            .await
            .expect("retire succeeds once the in-flight work drained");
        assert!(member.is_retired());
        // While the rebuild holds the exclusive guard, epoch-agnostic fan-out
        // (lifecycle_shared) cannot interleave with its engine resolution.
        let fan_out = tokio::time::timeout(
            std::time::Duration::from_millis(50),
            engine.lifecycle_shared(),
        )
        .await;
        assert!(fan_out.is_err(), "fan-out must wait for the rebuild's swap");
        drop(exclusive);
        let _fan_out =
            tokio::time::timeout(std::time::Duration::from_secs(5), engine.lifecycle_shared())
                .await
                .expect("fan-out proceeds after the swap");
    }

    /// #1473 (review): `begin_receiving_snapshot` holds the member's
    /// epoch-checked lifecycle guard across its orphan sweep AND the
    /// `truncate(true)` file open — not a bare `is_retired()` probe — so a
    /// predecessor receive parked in that open cannot truncate the successor's
    /// incoming snapshot on the shared dir (both members derive
    /// `incoming-<pid>-<seq>.tmp` from the same partition + process id, and
    /// each member's `recv_idx` restarts at 0, so the names collide).
    ///
    /// The interleaving this guards, deterministically: a receive is parked
    /// INSIDE the state machine's receive (an in-flight shared guard held by a
    /// detached task — the same shape as openraft's sm worker, which
    /// `Raft::shutdown` does NOT join) → retirement's exclusive acquisition
    /// must wait for it (it cannot construct the successor over the parked
    /// open) → the receive completes → retirement drains and retires the epoch
    /// → the successor's receive on the same dir is admitted under the new
    /// epoch, its file intact.
    #[tokio::test]
    async fn receive_holds_the_lifecycle_guard_through_the_sweep_and_file_open() {
        let engine = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let member = RaftPartition::bootstrap_single(0, 0, "127.0.0.1:0".into(), engine.clone())
            .await
            .expect("boot a volatile single voter");
        // A predecessor state machine on a DURABLE-style dir (survives retire).
        let dir = unique_log_dir("1473-recv");
        std::fs::create_dir_all(&dir).expect("recv dir");
        let mut predecessor: Arc<PartitionStateMachine> = Arc::new(
            PartitionStateMachine::new(
                engine.clone(),
                0,
                dir.clone(),
                0,
                false,
                Arc::new(AtomicU64::new(u64::MAX)),
            )
            .expect("predecessor sm"),
        );

        // Park a receive INSIDE the sweep+open on a detached task (the sm-worker
        // shape): it takes the member's shared lifecycle guard — exactly what
        // `begin_receiving_snapshot` now holds across the sweep + file open —
        // and keeps it held open-ended, modelling the predecessor suspended in
        // its file open. A barrier reports it parked.
        let (opened_tx, opened_rx) = tokio::sync::oneshot::channel::<()>();
        let (release_tx, release_rx) = tokio::sync::oneshot::channel::<()>();
        let parked = {
            let sm = Arc::clone(&predecessor);
            tokio::spawn(async move {
                let mut sm = sm;
                // Hold the shared guard across the receive's sweep + file open,
                // as the #1473 fix does, and keep it held past the open.
                let _guard = sm.enter_lifecycle().await.expect("live epoch admitted");
                let _file = sm
                    .begin_receiving_snapshot()
                    .await
                    .expect("receive opens its incoming file");
                let _ = opened_tx.send(());
                let _ = release_rx.await;
            })
        };
        opened_rx
            .await
            .expect("the receive parked with its file open");

        // While that receive's guard is in flight, retirement's exclusive
        // acquisition cannot complete — the rebuild's successor construction is
        // never interleaved with the parked open.
        let blocked = member
            .retire_within(std::time::Duration::from_millis(50))
            .await;
        assert!(
            blocked.is_err(),
            "retirement must wait for a receive parked in its file open"
        );

        // Complete the parked receive; retirement now drains and retires.
        let _ = release_tx.send(());
        parked.await.expect("the parked receive finished");
        let exclusive = member
            .retire_within(std::time::Duration::from_secs(5))
            .await
            .expect("retire succeeds once the parked receive left");
        assert!(member.is_retired());
        drop(exclusive);

        // A SUCCESSOR bound to the new epoch receives into the same dir without
        // any predecessor open still able to truncate its file.
        let mut successor: Arc<PartitionStateMachine> = Arc::new(
            PartitionStateMachine::new(
                engine.clone(),
                0,
                dir.clone(),
                0,
                false,
                Arc::new(AtomicU64::new(u64::MAX)),
            )
            .expect("successor sm on the predecessor's durable dir"),
        );
        let incoming = successor
            .begin_receiving_snapshot()
            .await
            .expect("the successor receives under the new epoch");
        let incoming_path = incoming.path.clone();
        assert!(
            incoming_path.starts_with(&dir),
            "the successor's incoming file lives in the shared dir"
        );
        assert!(
            incoming_path.is_file(),
            "no predecessor open truncated the successor's incoming snapshot"
        );
        drop(incoming);
        // And the RETIRED predecessor is refused before it touches the dir.
        assert!(predecessor.begin_receiving_snapshot().await.is_err());
        let _ = std::fs::remove_dir_all(&dir);
    }
}
