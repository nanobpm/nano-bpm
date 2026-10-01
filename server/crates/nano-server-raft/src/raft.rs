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
    BasicNode, Config, Entry, EntryPayload, LogId, OptionalSend, RaftSnapshotBuilder, SnapshotMeta,
    StorageError, StorageIOError, StoredMembership, Vote,
};
use serde::{Deserialize, Serialize};

use crate::deepthi::DeepthiHandle;
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
        | E::UnsupportedMigration { .. } => (409, e.to_string()),
        other => (500, other.to_string()),
    }
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
        let inner = self.inner.lock().unwrap();
        Ok(inner.log.range(bounds).map(|(_, e)| e.clone()).collect())
    }
}

impl RaftLogStorage<RaftConfig> for MemLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<RaftConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap();
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
        self.inner.lock().unwrap().vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().vote)
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
            let mut inner = self.inner.lock().unwrap();
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
        let mut inner = self.inner.lock().unwrap();
        let _removed = inner.log.split_off(&log_id.index);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Drop everything up to and including `log_id.index`, keep the rest.
        let mut inner = self.inner.lock().unwrap();
        inner.last_purged = Some(log_id);
        inner.log = inner.log.split_off(&(log_id.index + 1));
        Ok(())
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        self.inner.lock().unwrap().committed = committed;
        Ok(())
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap().committed)
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
            snapshot_dir,
            node_id,
            evict_eligible,
            leader,
        })
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
            let guard = self.current_snapshot.lock().unwrap();
            match guard.as_ref() {
                Some(s) => s.path.clone(),
                None => return Ok(()),
            }
        };
        let captured: nanobpmn_engine_core::EngineSnapshot =
            tokio::task::spawn_blocking(move || -> std::io::Result<_> {
                let f = std::fs::File::open(&path)?;
                serde_json::from_reader(std::io::BufReader::new(f))
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            })
            .await??;
        self.engine
            .with(move |journal| {
                journal.restore_engine_from_snapshot(captured);
            })
            .await;
        Ok(())
    }
}

/// Filename prefix for the per-process, per-partition snapshot staging dirs a
/// volatile-log member anchors its snapshots under (see
/// [`PartitionStateMachine::temp_snapshot_dir`]). Shared with
/// [`sweep_orphaned_snapshot_dirs`] so creation and cleanup never drift.
const RAFT_SNAPSHOT_DIR_PREFIX: &str = "nanobpmn-raftsnap-";

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
pub struct PartitionSnapshotBuilder {
    sm: Arc<PartitionStateMachine>,
    captured: nanobpmn_engine_core::EngineSnapshot,
    last_applied: Option<LogId<NodeId>>,
    last_membership: StoredMembership<NodeId, BasicNode>,
}

impl RaftSnapshotBuilder<RaftConfig> for PartitionSnapshotBuilder {
    async fn build_snapshot(&mut self) -> Result<Snapshot<RaftConfig>, StorageError<NodeId>> {
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

        // Stream the state capture straight to disk (bounded memory) rather than
        // building a full `Vec<u8>` plus a second cached clone. `serde_json` here
        // runs the same blocking serialize the old `to_vec` did — but into a
        // buffered writer, so the peak transient is one buffer, not the whole
        // serialized state twice.
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
        let path = self
            .sm
            .snapshot_dir
            .join(format!("snap-{snapshot_idx}.bin"));
        let file = std::fs::File::create(&path)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        let mut writer = std::io::BufWriter::new(file);
        let serialize_start = std::time::Instant::now();
        serde_json::to_writer(&mut writer, &self.captured)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        let file = writer
            .into_inner()
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e.into_error()))?;
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
        let stored = StoredSnapshot {
            meta: meta.clone(),
            path: path.clone(),
        };
        write_snapshot_ptr(&self.sm.snapshot_dir, &stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;

        // Publish as the current snapshot and unlink the file it replaces.
        let previous = self.sm.current_snapshot.lock().unwrap().replace(stored);
        if let Some(previous) = previous.filter(|p| p.path != path) {
            let _ = std::fs::remove_file(&previous.path);
        }

        let tokio_file = tokio::fs::File::open(&path)
            .await
            .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
        Ok(Snapshot {
            meta,
            snapshot: Box::new(SnapshotFile {
                file: tokio_file,
                path,
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
        let inner = self.inner.lock().unwrap();
        Ok((inner.last_applied, inner.last_membership.clone()))
    }

    async fn apply<I>(
        &mut self,
        entries: I,
    ) -> Result<Vec<ReplicatedResponse>, StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<RaftConfig>> + Send,
    {
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
                    self.inner.lock().unwrap().last_applied = Some(log_id);
                    responses.push(ReplicatedResponse::default());
                }
                EntryPayload::Normal(batch) => {
                    // Phase 1: apply every command in the batch in ONE actor hop,
                    // returning per-command (events, commit) or the engine rejection.
                    type ApplyOutcome = Result<
                        (Arc<Vec<Event>>, crate::journal::Commit),
                        nanobpmn_engine_core::EngineError,
                    >;
                    let outcomes: Vec<ApplyOutcome> = self
                        .engine
                        .with(move |journal| {
                            batch
                                .items
                                .into_iter()
                                .map(|ReplicatedCommand { command, now }| {
                                    // Per-command actor profiling (off unless
                                    // NANOBPM_CMD_PROFILE): time + allocated-byte
                                    // delta by command kind, on the engine thread.
                                    let timer = crate::cmd_profile::start();
                                    let kind = command.kind();
                                    let outcome = journal.apply_command_at(command, now);
                                    crate::cmd_profile::finish(timer, kind);
                                    outcome
                                })
                                .collect()
                        })
                        .await;

                    // Phase 2: await the durable barriers (now coalesced by the
                    // group-commit writer) and build the per-command responses.
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
                            Ok((events, commit)) => {
                                commit.wait().await;
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
                            // leader surfaces the mapped rejection to its client.
                            Err(e) => {
                                items.push(ReplicatedItem {
                                    events: Vec::new(),
                                    error: Some(engine_error_status(&e)),
                                });
                            }
                        }
                    }
                    if !terminal.is_empty() {
                        self.engine.spawn_job(move |journal| {
                            journal.evict_instances(&terminal);
                        });
                    }
                    self.inner.lock().unwrap().last_applied = Some(log_id);
                    responses.push(ReplicatedResponse { items });
                }
                EntryPayload::Membership(mem) => {
                    let mut inner = self.inner.lock().unwrap();
                    inner.last_applied = Some(log_id);
                    inner.last_membership = StoredMembership::new(Some(log_id), mem);
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
        let captured = self.engine.with(|journal| journal.engine_snapshot()).await;
        let (last_applied, last_membership) = {
            let inner = self.inner.lock().unwrap();
            (inner.last_applied, inner.last_membership.clone())
        };
        PartitionSnapshotBuilder {
            sm: self.clone(),
            captured,
            last_applied,
            last_membership,
        }
    }

    async fn begin_receiving_snapshot(
        &mut self,
    ) -> Result<Box<SnapshotFile>, StorageError<NodeId>> {
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
        let file = tokio::fs::OpenOptions::new()
            .create(true)
            .read(true)
            .write(true)
            .truncate(true)
            .open(&path)
            .await
            .map_err(|e| StorageIOError::write_snapshot(None, &e))?;
        Ok(Box::new(SnapshotFile { file, path }))
    }

    async fn install_snapshot(
        &mut self,
        meta: &SnapshotMeta<NodeId, BasicNode>,
        snapshot: Box<SnapshotFile>,
    ) -> Result<(), StorageError<NodeId>> {
        let SnapshotFile { file, path } = *snapshot;

        // Stream-deserialize the received file from a blocking task (bounded
        // memory: a BufReader, not the whole body as a `Vec<u8>`). openraft leaves
        // the write cursor at the end, so rewind first.
        let std_file = file.into_std().await;
        let captured: nanobpmn_engine_core::EngineSnapshot = tokio::task::spawn_blocking(
            move || -> std::io::Result<nanobpmn_engine_core::EngineSnapshot> {
                use std::io::Seek;
                let mut f = std_file;
                f.seek(std::io::SeekFrom::Start(0))?;
                serde_json::from_reader(std::io::BufReader::new(f))
                    .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
            },
        )
        .await
        .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?
        .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;

        // Rebuild the engine actor's state from the captured snapshot IN PLACE.
        // The engine journal is in-memory under Raft (the Raft log is the durable
        // tier), so adopting the snapshot's engine state is the install — but the
        // journal's read-model exporter wiring (and partition id / var store /
        // spill tiers) MUST survive it, or an owned partition catching up via a
        // snapshot install stops projecting and leaks its completed backlog.
        self.engine
            .with(move |journal| {
                journal.restore_engine_from_snapshot(captured);
            })
            .await;

        {
            let mut inner = self.inner.lock().unwrap();
            inner.last_applied = meta.last_log_id;
            inner.last_membership = meta.last_membership.clone();
        }

        // Promote the received file to the current snapshot (a rename within the
        // same dir — cheap, no re-serialize, no extra copy) and drop the old one.
        let snapshot_idx = self.snapshot_idx.fetch_add(1, Ordering::Relaxed) + 1;
        let current_path = self
            .snapshot_dir
            .join(format!("snap-installed-{snapshot_idx}.bin"));
        std::fs::rename(&path, &current_path)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        // fsync the promoted file, then record it as the durable current snapshot
        // BEFORE unlinking the one it replaces — an installed snapshot is just as
        // much a restart recovery point as a locally-built one, and the log store
        // will purge below it (see [`PersistedSnapshotPtr`]).
        if let Ok(f) = std::fs::File::open(&current_path) {
            let _ = f.sync_all();
        }
        let stored = StoredSnapshot {
            meta: meta.clone(),
            path: current_path,
        };
        write_snapshot_ptr(&self.snapshot_dir, &stored)
            .map_err(|e| StorageIOError::write_snapshot(Some(meta.signature()), &e))?;
        let previous = self.current_snapshot.lock().unwrap().replace(stored);
        if let Some(previous) = previous {
            let _ = std::fs::remove_file(&previous.path);
        }
        Ok(())
    }

    async fn get_current_snapshot(
        &mut self,
    ) -> Result<Option<Snapshot<RaftConfig>>, StorageError<NodeId>> {
        // Copy the small (meta, path) pair out from under the lock so the file
        // open can `.await` without holding the std mutex.
        let entry = {
            let guard = self.current_snapshot.lock().unwrap();
            guard.as_ref().map(|s| (s.meta.clone(), s.path.clone()))
        };
        match entry {
            Some((meta, path)) => {
                let file = tokio::fs::File::open(&path)
                    .await
                    .map_err(|e| StorageIOError::read_snapshot(Some(meta.signature()), &e))?;
                Ok(Some(Snapshot {
                    meta,
                    snapshot: Box::new(SnapshotFile { file, path }),
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
    SNAPSHOT_BUILD_SEM
        .get_or_init(|| tokio::sync::Semaphore::new(snapshot_build_concurrency() as usize))
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
}

impl Batcher {
    fn spawn(raft: openraft::Raft<RaftConfig>) -> Self {
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
        Self { hi_tx, lo_tx }
    }

    async fn submit(&self, command: Command, now: u64) -> Result<ReplicatedItem, ProposeError> {
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
        let state_machine = Arc::new(PartitionStateMachine::new_temp(engine, partition_id)?);
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

        let batcher = Batcher::spawn(raft.clone());
        Ok(Self {
            raft,
            node_id,
            partition_id,
            batcher,
            log_bytes: Arc::new(AtomicI64::new(0)),
            snapshot_progress,
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
        let state_machine = Arc::new(PartitionStateMachine::new(
            engine,
            partition_id,
            log_dir.join("snapshots"),
            node_id,
            false,
            Arc::new(AtomicU64::new(u64::MAX)),
        )?);
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

        let batcher = Batcher::spawn(raft.clone());
        Ok(Self {
            raft,
            node_id,
            partition_id,
            batcher,
            log_bytes,
            snapshot_progress,
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
        let snapshot_dir = match log_dir.as_ref() {
            Some(dir) => dir.join("snapshots"),
            None => PartitionStateMachine::temp_snapshot_dir(partition_id),
        };
        // Live current-leader signal the state machine's terminal-eviction gate
        // reads. Only meaningful for an evict-eligible follower; kept fresh by a
        // metrics watcher spawned below once the Raft handle exists.
        let leader = Arc::new(AtomicU64::new(u64::MAX));
        let state_machine = Arc::new(PartitionStateMachine::new(
            engine,
            partition_id,
            snapshot_dir,
            node_id,
            evict_eligible,
            leader.clone(),
        )?);
        // Restore the engine from the durable current snapshot (if any) BEFORE
        // openraft is constructed: with a compacted log the snapshot holds the
        // purged prefix, and the state machine already carries its applied
        // metadata, so openraft's `get_initial_state` reconciles cleanly and only
        // replays the post-snapshot log tail. Without this a rejoining node whose
        // log was purged fails to host the partition entirely.
        state_machine.restore_from_current_snapshot().await?;
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
        let batcher = Batcher::spawn(raft.clone());
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
            .unwrap()
            .insert(part.partition_id, part);
    }

    /// The hosted group for `partition`, if this node replicates it.
    pub fn get(&self, partition: u64) -> Option<Arc<RaftPartition>> {
        self.partitions.lock().unwrap().get(&partition).cloned()
    }

    /// Whether this node hosts no Raft groups (the non-Raft default).
    pub fn is_empty(&self) -> bool {
        self.partitions.lock().unwrap().is_empty()
    }

    /// A snapshot of every hosted partition, ordered by partition id. Used by the
    /// `/debug/raft` diagnostic endpoint to dump per-partition replication indices.
    pub fn all(&self) -> Vec<Arc<RaftPartition>> {
        let mut parts: Vec<Arc<RaftPartition>> =
            self.partitions.lock().unwrap().values().cloned().collect();
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
    fn snapshot_build_concurrency_defaults_and_floors() {
        // Env-free default is 2 concurrent builds (steady-state co-hosted partitions
        // rarely coincide thanks to the jitter, so a small cap is unobtrusive).
        // The env var is read at process start via the OnceLock, so here we only
        // assert the pure default + floor of the helper.
        let c = snapshot_build_concurrency();
        assert!(c >= 1, "concurrency must be floored at 1, got {c}");
        // The semaphore is sized from the same helper, so it always has >=1 permit
        // (acquire_many(concurrency) for the exclusive recovery path can succeed).
        assert!(snapshot_build_sem().available_permits() >= 1);
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

        // The body is a compact EngineSnapshot on disk, not an event log: reading
        // the file-backed snapshot back deserializes straight into an
        // EngineSnapshot.
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
            serde_json::from_slice(&bytes).expect("snapshot body is a state capture");

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
            src_sm.inner.lock().unwrap().last_applied = Some(applied);
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
}
