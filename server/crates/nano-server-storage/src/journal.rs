//! An append-only event journal that makes the embedded engine durable.
//!
//! The engine itself is in-memory and event-sourced ([`Engine::apply_command_at`]
//! returns the complete, ordered list of events a command produced). This
//! [`Journal`] wraps the engine and persists those events to a newline-delimited
//! JSON log. Writes are handed to a dedicated background thread that
//! **group-commits** — batching every concurrently in-flight command into a
//! single `write` + `fsync` — and a command is acknowledged only once its events
//! are fsynced, so anything the server returns `200` for survives a crash. On
//! startup the log is replayed through [`Engine::replay`] to reconstruct state
//! and the key generator.
//!
//! **Activation locks are deliberately not journaled.** Job activation
//! (`activateJobs`) and lock expiry are *volatile lease state*: a crash forfeits
//! every lock, returning uncompleted jobs to the activatable pool. Recording
//! only durable business facts keeps the log small and gives a clean recovery
//! semantic — after a restart, workers simply re-activate.

use std::collections::{HashMap, HashSet};
use std::io;
use std::path::Path;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{self, Receiver, RecvTimeoutError, Sender};
use std::sync::{Arc, Mutex};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

use nanobpmn_engine_core::{
    ActivatedJob, ClusterVariables, Command, Engine, EngineError, Event, Incident, Key,
    ProcessInstance, State, Value,
};
use tokio::sync::oneshot;

use crate::coldspill::ColdIndex;
use crate::seglog::{ActiveSegment, SegShared};
use crate::varspill::VarSpillStore;
use crate::varstore::VarStore;

/// A variable-upsert delta: each changed instance's current variable map as a
/// cheap `Arc` clone (the deep read happens off the engine thread when the host
/// serializes it into the durable var store).
type VarUpsertDelta = Vec<(Key, Arc<HashMap<String, Value>>)>;

/// The result of a lean-snapshot checkpoint (see
/// [`Journal::snapshot_and_rotate_lean`]): the control-only snapshot, the event
/// count it covers, the changed instances' current variable maps to upsert into
/// the durable var store (cheap `Arc` clones — the deep read happens off the
/// engine thread), and the terminal instances' keys to delete from it.
type LeanCheckpoint = (
    nanobpmn_engine_core::EngineSnapshot,
    u64,
    VarUpsertDelta,
    Vec<Key>,
);

/// A durable-write request handed to the background journal writer: the
/// newline-terminated, serialized bytes for one command's events, the number of
/// events they encode (so the segmented writer can track absolute positions),
/// plus a one-shot sender signalled once those bytes are fsynced to disk.
/// Shared cell holding the read-model exporter senders for a segmented shared
/// writer, keyed by global partition id. The writer thread and every
/// shared-backed [`Journal`] hold an `Arc` clone; a shared-backed
/// [`Journal::set_exporter`] registers its partition's sender, and the writer
/// forwards each committed command's events through the matching partition's
/// sender in fsync (log) order (see [`forward_shared_export`]). Sharding the map
/// by partition lets each owned partition drive its own read-store shard +
/// exporter thread, so projection scales with cores. Empty for the non-shared
/// journals (which forward from `persist()` and leave this untouched).
///
/// The value pairs each partition's sender with its exporter-queue byte gauge
/// (`Arc<AtomicU64>`): the writer increments it by each forwarded command's
/// serialized size before the send, and the exporter thread decrements it after
/// the batch is projected. The create-admission path reads the same gauge to
/// steer creates away from — and, once every local shard is saturated, shed —
/// so the resident exporter backlog stays bounded under a large-variable flood
/// (see `ExporterBackpressure`).
type ExporterCell = Arc<Mutex<HashMap<u64, (Sender<ExportBatch>, Arc<AtomicU64>)>>>;

/// A read-model export item handed to an exporter thread: a command's events
/// plus `bytes`, an estimate of their resident cost (the serialized journal
/// bytes for those events) used to account the exporter queue's size for
/// backpressure. `bytes` is `0` on the non-shared (in-memory / legacy
/// single-file) path, which does not participate in exporter-queue accounting.
pub struct ExportBatch {
    pub events: Arc<Vec<Event>>,
    pub bytes: u32,
}

struct WriteRequest {
    bytes: Vec<u8>,
    events: usize,
    /// Global partition id that produced these events (0 on the single-partition
    /// path). Lets the shared multi-partition writer attribute each write to a
    /// partition's cumulative count for per-partition seal boundaries.
    partition: u64,
    /// The command's events, shared by `Arc`. `Some` only for a journal backed by
    /// the shared multi-partition writer with a wired exporter cell: the writer
    /// forwards them to the read-model exporter in fsync (log) order, so
    /// `exported_position` stays a true global prefix. `None` everywhere else
    /// (those paths forward from `persist()` directly).
    events_arc: Option<Arc<Vec<Event>>>,
    ack: oneshot::Sender<()>,
}

/// A message to the background journal writer: either a durable write, or a
/// control request to **seal** (rotate) the active segment at the current
/// boundary and report it. Routing both through one ordered channel guarantees
/// a rotate is processed exactly between the writes before and after it, so a
/// snapshot taken on the engine actor covers precisely the sealed segment(s).
enum WriterMsg {
    Write(WriteRequest),
    /// Seal the active segment now; reply with the sealed boundary.
    Rotate(oneshot::Sender<crate::seglog::SealInfo>),
}

/// A handle to the durability of a single write. Awaiting [`Commit::wait`]
/// resolves once the command's events have been group-committed (written and
/// fsynced) by the background writer thread. A `Ready` commit is already durable
/// (an in-memory journal, or a command that produced no events).
#[must_use = "await the commit to guarantee the write is durable before responding"]
pub struct Commit(CommitInner);

enum CommitInner {
    Ready,
    Pending(oneshot::Receiver<()>),
}

impl Commit {
    pub fn ready() -> Self {
        Commit(CommitInner::Ready)
    }

    /// Waits until the write backing this commit has been fsynced. If the writer
    /// thread is gone (shutdown), it resolves immediately rather than hanging.
    pub async fn wait(self) {
        if let CommitInner::Pending(rx) = self.0 {
            let start = std::time::Instant::now();
            let _ = rx.await;
            crate::metrics::record_commit_wait(start.elapsed());
        }
    }

    /// Blocks the current thread until the write backing this commit has been
    /// fsynced. For synchronous callers (tests, startup seeding) that cannot
    /// `.await`; must NOT be called from within an async runtime. Resolves
    /// immediately if the writer thread is gone (shutdown) rather than hanging.
    pub fn blocking_wait(self) {
        if let CommitInner::Pending(rx) = self.0 {
            let start = std::time::Instant::now();
            let _ = rx.blocking_recv();
            crate::metrics::record_commit_wait(start.elapsed());
        }
    }
}

/// How a snapshot fold treats an off-heap row that is **definitively absent**
/// from a wired, readable store (#1383). Unreadable stores always fail closed.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum OrphanPolicy {
    /// Fail the capture (Raft: the snapshot is replicated and purges the log).
    Strict,
    /// Quarantine the orphan and proceed (local journal rotation only).
    Quarantine,
}

/// Blocking-key tracking for the snapshot fold (#1385). A fail-closed fold
/// aborts at the first unreadable payload; that key is recorded as the current
/// blocker and its ERROR is logged once per outage (until a fold succeeds)
/// instead of on every snapshot tick, which is what buried #1383 for 23 days.
#[derive(Default)]
struct SnapshotBlockers {
    /// Blockers of the most recent fold attempt (empty after a success).
    current: Mutex<Vec<Key>>,
    /// Keys already logged during the ongoing outage.
    logged: Mutex<HashSet<Key>>,
    /// Repeat blocker log lines suppressed so far.
    suppressed: AtomicU64,
}

/// The engine plus its durable event log.
pub struct Journal {
    engine: Engine,
    /// Global partition id this journal's engine owns (0 for single-partition).
    /// Tags each durable write so the shared multi-partition writer can track
    /// per-partition cumulative counts, and selects this partition's covered
    /// count out of a seal boundary.
    partition_id: u64,
    /// `None` for an in-memory (non-persistent) journal; otherwise the channel to
    /// the background writer thread that owns the log file.
    writer: Option<Sender<WriterMsg>>,
    /// Joined on drop so any not-yet-acked writes are flushed before the journal
    /// goes away (matters for synchronous callers that never await the commit).
    writer_thread: Option<JoinHandle<()>>,
    /// Shared seal/boundary state when this journal is backed by a *segmented*
    /// log (the single-partition bounded-disk path). `None` for in-memory and the
    /// legacy single-file paths. Lets the snapshot/compaction task read segment
    /// boundaries and drives [`Journal::snapshot_and_rotate`].
    seg: Option<Arc<SegShared>>,
    /// Channel to the read-model exporter thread, if one is wired. Every command's
    /// journaled events are forwarded here (in command order: the actor applies
    /// commands serially) to be projected into the read store. The events are
    /// shared with the command's caller via `Arc`, so forwarding them costs only a
    /// refcount bump — the 50 KB variable payloads are never deep-copied on the
    /// single command thread.
    exporter: Option<Sender<ExportBatch>>,
    /// For a journal backed by the shared multi-partition writer: the writer's
    /// exporter cell. `set_exporter` stores the exporter here instead of in
    /// `self.exporter`, so the SHARED WRITER forwards events to the read model in
    /// log order (making `exported_position` a true global prefix) and
    /// `persist()` does not double-project. `None` for every other path (which
    /// forward from `persist()`).
    shared_exporter: Option<ExporterCell>,
    /// `true` when the journal started with no prior log, so the host knows it
    /// should seed any initial deployments.
    fresh: bool,
    /// Optional disk-backed variable spill. Sheds the variables of the oldest
    /// *active* backlog instances to disk (rehydrated on job activation), bounding
    /// the RAM held by a large active backlog. `None` keeps every payload resident
    /// (the original behaviour). The [`VarSpillTrigger`] decides *when* to shed —
    /// a fixed instance-count budget, or adaptively under real RAM pressure.
    spill: Option<VarSpill>,
    /// Optional cold spill: evicts whole *dormant* instances (control state and
    /// all) to the same disk-backed store, keeping only a slim resident routing
    /// index ([`ColdIndex`]) so an event can rehydrate the owning instance on
    /// demand. Triggered by hot-RAM pressure (resident bytes crossing
    /// `high_water`), it sheds the oldest dormant instances until back under
    /// `low_water`. `None` keeps every instance resident. This is the long-lived,
    /// low-throughput counterpart to variable spill: where variable spill bounds a
    /// large *active* backlog, cold spill bounds a large *parked* one.
    cold: Option<ColdSpill>,
    /// The authoritative durable variable store, when lean-snapshot mode is on.
    /// Under lean mode the periodic snapshot carries only control state; every
    /// live instance's current variables live here instead. Variable spill
    /// write-through, job-activation rehydration, terminal forget, and the
    /// periodic checkpoint all target this store. `None` keeps the classic
    /// full-variable snapshot (and the destructive [`VarSpillStore`] spill cache).
    varstore: Option<Arc<VarStore>>,
    /// Countdown that amortises the O(N) spillable scan in [`maybe_spill`]. While
    /// the resident set stays over the spill budget, the precise scan/shed runs
    /// only once every [`SPILL_CHECK_INTERVAL`] commands instead of on every
    /// apply; between runs the hot path is O(1). Reset to zero whenever the
    /// resident set drops back within budget so the next overflow is caught
    /// immediately.
    spill_check_skip: u32,
    /// Host-injected [`ClusterVariables`] handle shared with this journal's
    /// engine. Held here (in addition to being installed on the engine) so it can
    /// be re-installed whenever the engine is rebuilt from a snapshot — cluster
    /// variables are external configuration, so a `from_snapshot`/replay engine
    /// starts with an empty set and must be re-linked to the live host handle,
    /// exactly as `lenient_completion` is preserved across a snapshot load.
    cluster_variables: ClusterVariables,
    /// Keys of instances already quarantined as **off-heap orphans** (#1383): a
    /// spilled instance whose variable row, or a cold instance whose snapshot
    /// row, is *definitively absent* from a readable store. Its payload is
    /// irrecoverably lost, so the snapshot fold proceeds without it instead of
    /// failing closed forever (which blocked every snapshot — and so all
    /// compaction — for 23 days on merlin). Held only to log each orphan once
    /// rather than on every snapshot tick.
    quarantined: Mutex<HashSet<Key>>,
    /// Count of off-heap rehydration reads that failed as **unreadable**
    /// (SQLite/IO error or an undecodable row, #1388). The row and routing entry
    /// are kept for a retry; this makes each such failure observable.
    offheap_read_failures: AtomicU64,
    /// Instances whose off-heap payload aborted the latest snapshot fold, and
    /// log-dedupe state for them (#1385). Surfaced on `/health` so an operator
    /// can name what blocks compaction instead of grepping a per-tick ERROR.
    snapshot_blockers: SnapshotBlockers,
    /// `true` when this journal's commands are applied as replicated (Raft)
    /// state-machine application (#1384). There, a quarantine discovered while
    /// applying a command must stay deterministic across replicas, so it is
    /// observed (log + metric) only and never raises the `VariablesLost`
    /// incident (which would mint a replica-local key + wall-clock timestamp).
    replicated_apply: bool,
    /// Per-job-type rotation cursor for cold activation candidates (#1388): the
    /// last key whose cold row was unreadable, so the next poll starts after it
    /// and an unreadable oldest row cannot starve healthy later instances.
    activation_cursor: HashMap<String, Key>,
    /// The Raft entry currently being applied (#1405): `Some(buffer)` between
    /// [`begin_raft_entry`](Self::begin_raft_entry) and
    /// [`commit_raft_entry`](Self::commit_raft_entry) /
    /// [`abort_raft_entry`](Self::abort_raft_entry). While armed, each applied
    /// command's events are BUFFERED here instead of written, so the whole
    /// entry — plus its [`Event::RaftEntryApplied`] record — is journaled as
    /// ONE atomic frame. `None` outside Raft apply (the direct REST /
    /// single-node path journals per command as before).
    raft_entry: Option<Vec<Event>>,
    /// Set by [`abort_raft_entry`](Self::abort_raft_entry) (#1405): the live
    /// engine may hold a refused entry's partial, NON-durable effects, so this
    /// journal is FENCED until the process rebuilds it from disk — no further
    /// mutation, journal write, spill, or snapshot/checkpoint capture. Without
    /// the fence a later checkpoint (once the cold-read failure clears) could
    /// persist that partial prefix under the previous applied index, and a
    /// restart would re-execute the entry on top of it.
    fenced: bool,
    /// Test-only instrumentation: how many times the cold-descendant traversal
    /// scanned the resident instance set per visited descendant (the O(N × D)
    /// shape Copilot flagged on #1418). The indexed traversal never scans, so a
    /// regression test pins this at zero.
    #[cfg(test)]
    resident_child_scans: usize,
}

/// Extra cold reads an activation poll may spend past `max_jobs` to skip
/// unreadable rows (#1388).
const ACTIVATION_EXTRA_READS: usize = 8;

/// Outcome of [`Journal::rehydrate_cold`] (#1388). `Unreadable` is the
/// transient case a command must fail closed on rather than run against state
/// missing the instance.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[must_use]
enum ColdRehydration {
    /// Cold spill unset, or `key` is not cold — nothing to do.
    NotCold,
    /// The instance is resident again.
    Restored,
    /// The store is readable but has no row: lost, quarantined.
    Absent,
    /// The row could not be read; the instance stays cold and routable.
    Unreadable,
}

/// Cold-spill state held by a [`Journal`]: the shared disk store, the resident
/// routing index, and the resident-byte watermarks that gate the sweep.
struct ColdSpill {
    store: Arc<VarSpillStore>,
    index: ColdIndex,
    high_water: u64,
    low_water: u64,
    /// Hysteresis latch: set once resident crosses `high_water` and cleared only
    /// when it falls back under `low_water`. While latched the budgeted sweep
    /// keeps shedding across ticks toward `low_water` (the OOM guard's reclaim
    /// target) rather than stopping the moment resident dips under `high_water`,
    /// which would flap the sweep on and off right at the mark.
    shedding: bool,
}

/// Variable-spill state held by a [`Journal`]: the shared disk store and the
/// policy that decides *when* the oldest active-backlog variables are shed.
struct VarSpill {
    store: Arc<VarSpillStore>,
    trigger: VarSpillTrigger,
}

/// When variable spill sheds an active instance's variables to disk.
enum VarSpillTrigger {
    /// Fixed instance-count budget, evaluated on every command. Simple and
    /// deterministic, but a *count* is a poor proxy for *bytes* (512 tiny-var
    /// instances is trivial RAM; 512 large-var instances is not), so it must be
    /// tuned conservatively and can throttle throughput needlessly.
    Budget(usize),
    /// Adaptive: shed strictly on **measured memory** so a healthy large working
    /// set keeps full throughput while genuine memory pressure is bounded to the
    /// reclaim target. Instance count is not a signal (a poor proxy for bytes).
    ///
    /// - Below `high_water` with free RAM: never spill — full throughput, however
    ///   large the (stable) working set.
    /// - Resident `>= high_water`: shed toward `low_water` — the OOM guard.
    /// - Live system-available memory below `reserve`: shed toward `floor` (free
    ///   RAM is contended by another tenant, so reclaim harder).
    ///
    /// `floor` is the aggressive reclaim target under a `reserve` breach;
    /// `low_water` the gentle one under a plain high-water breach (hysteresis).
    /// `hard_cap` is the per-command instance-count backstop for a create-flood
    /// *between* the 500 ms sweeps (`0` disables it, relying solely on the
    /// sweep). `reserve == 0` disables the live-available-memory guard.
    Adaptive {
        floor: u64,
        high_water: u64,
        low_water: u64,
        reserve: u64,
        hard_cap: usize,
    },
}

impl VarSpillTrigger {
    /// The resident spill-candidate count above which the *per-command* path
    /// sheds. `Budget` sheds at its budget; `Adaptive` sheds only at its emergency
    /// `hard_cap` (its normal trigger is the RSS sweep). `None` means the
    /// per-command path never sheds.
    fn per_command_cap(&self) -> Option<usize> {
        match self {
            VarSpillTrigger::Budget(budget) => Some(*budget),
            VarSpillTrigger::Adaptive { hard_cap, .. } => (*hard_cap != 0).then_some(*hard_cap),
        }
    }
}

/// Upper bound on how many writes one group-commit batch will accumulate before
/// forcing the fsync, regardless of the linger window. Bounds worst-case commit
/// latency and the staging buffer when offered load is very high.
const MAX_GROUP_BATCH: usize = 8192;

/// How many commands the journal skips between precise spillable scans while the
/// resident set stays over the spill budget. Variable spill is a soft memory
/// bound (payloads stay durable in the journal), so amortising the O(N) scan
/// across this many applies keeps the hot path O(1) under a deep backlog while
/// bounding memory overshoot to at most this many commands' worth of newly
/// spillable variables.
const SPILL_CHECK_INTERVAL: u32 = 256;

/// How many instances' variables the adaptive RAM-pressure sweep sheds per batch
/// before re-measuring resident bytes. Keeps the shed responsive to the
/// watermark (stop as soon as we drop under `low_water`) without re-reading RSS —
/// an epoch-advance + stat — after every single instance.
const VAR_SPILL_SWEEP_BATCH: usize = 512;

/// Futile-shed guard for the memory-driven spill sweep: variable spill only runs
/// when the resident variable payloads it *could* shed are at least
/// `overshoot / VAR_SPILL_MIN_RELIEF_DIVISOR` bytes — i.e. spilling can relieve
/// at least ~1/8 of the amount by which resident memory overshoots the reclaim
/// target. Below that, variables are not the memory driver (control state is, or
/// the payloads are tiny), so shedding them would only thrash the backlog for
/// negligible relief; the residual is cold spill / admission backpressure's job.
/// This replaces the old per-batch RSS-delta bail, which mis-fired under
/// concurrent inbound allocation (new allocations masked the shed). Purely a
/// *byte* comparison — instance count never enters the decision.
const VAR_SPILL_MIN_RELIEF_DIVISOR: u64 = 8;

/// Per-tick wall-clock budget for the adaptive RAM-pressure spill sweeps
/// ([`Journal::maybe_var_spill_pressure`] / [`Journal::maybe_cold_spill`]). Both
/// run on the single-writer engine actor via the periodic tick. The previous
/// design looped — shedding a batch, then `shrink()` (ten `shrink_to_fit` map
/// reallocs) + jemalloc `purge()` + a resident re-read after *every* batch —
/// until resident memory fell under the reclaim target. Under a large active
/// backlog that held the writer for >100 ms per tick (measured 123 ms/call,
/// 2.4 MB alloc/call), starving instance creation and job completion: the
/// congestion-collapse hot path. Bounding each sweep to this budget caps the
/// hold; successive ticks resume shedding where the last left off, so relief is
/// spread across ticks instead of one multi-hundred-ms stall. Bounded intake
/// (admission control) keeps the backlog from outrunning the per-tick shed.
const SPILL_TICK_BUDGET: std::time::Duration = std::time::Duration::from_millis(8);

/// The background journal writer: blocks for the next request, drains every
/// other request already queued, then **group-commits** the whole batch in a
/// single `write` + `fsync` before signalling each command's commit. Batching
/// amortizes one fsync across all concurrently in-flight writes.
///
/// `linger` is an optional group-commit delay (à la Postgres `commit_delay` /
/// MySQL binlog group commit). When non-zero, after draining the instantly
/// available writes the writer waits up to `linger` for *more* writes to arrive
/// before committing. This breaks the closed-loop pathology where each client
/// blocks on its own `fsync` ack, so only one write is ever queued at a time and
/// the batch never grows past 1 — leaving throughput pinned at one commit per
/// fsync latency. A small linger lets many in-flight clients coalesce into one
/// fsync, trading a bounded latency increase for a large throughput gain on
/// fsync-latency-bound workloads. Zero (the default) preserves the original
/// drain-only behaviour exactly.
///
/// A write or fsync failure is unrecoverable — the in-memory engine has already
/// advanced past the durable log — so the process is aborted rather than risk
/// acknowledging or serving non-durable state (mirrors the previous
/// panic-on-I/O-error contract).
/// Forwards a shared-writer-backed command's events to the read-model exporter
/// (if wired) in fsync/log order. A no-op when `events_arc` is `None` (the
/// non-shared paths forward from `persist()` instead). An `events_arc` present
/// with no wired exporter cell is an ordering bug: the exporter must be wired
/// before any shared write is served; we debug-assert and skip (boot catch-up
/// covers everything written before the wire, so this can only be a real bug).
/// Forwards a shared-writer-backed command's events to the read-model exporter
/// shard for `partition` (if wired) in fsync/log order. A no-op when `events_arc`
/// is `None` (the non-shared paths forward from `persist()` instead). An
/// `events_arc` present with no wired exporter sender for the partition is an
/// ordering bug: every owned partition's exporter must be wired before any shared
/// write is served; we debug-assert and skip (boot catch-up covers everything
/// written before the wire, so this can only be a real bug).
fn forward_shared_export(
    exporter: &Option<ExporterCell>,
    partition: u64,
    events_arc: Option<Arc<Vec<Event>>>,
    bytes: u32,
) {
    let Some(events) = events_arc else { return };
    if events.is_empty() {
        return;
    }
    let Some(cell) = exporter else {
        debug_assert!(false, "shared write with no exporter cell wired");
        return;
    };
    if let Some((tx, queued)) = cell
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .get(&partition)
    {
        // Account the queued bytes BEFORE the send so the create-admission gauge
        // never lags behind a queue the exporter has not yet drained; the
        // exporter subtracts the same amount once the batch is projected.
        queued.fetch_add(bytes as u64, Ordering::Relaxed);
        let _ = tx.send(ExportBatch { events, bytes });
    } else {
        debug_assert!(false, "shared write before exporter was set for partition");
    }
}

fn writer_loop(
    mut seg: ActiveSegment,
    rx: Receiver<WriterMsg>,
    linger: Duration,
    exporter: Option<ExporterCell>,
) {
    loop {
        let idle_start = Instant::now();
        let Ok(first) = rx.recv() else { break };
        let idle = idle_start.elapsed();
        let busy_start = Instant::now();

        let mut batch: Vec<WriteRequest> = Vec::new();
        let mut pending_rotate: Option<oneshot::Sender<crate::seglog::SealInfo>> = None;
        match first {
            WriterMsg::Write(w) => batch.push(w),
            WriterMsg::Rotate(reply) => pending_rotate = Some(reply),
        }

        // Drain queued writes into this batch, stopping at a rotate (handled
        // after the batch is durable) or the group-commit cap.
        if pending_rotate.is_none() {
            while batch.len() < MAX_GROUP_BATCH {
                match rx.try_recv() {
                    Ok(WriterMsg::Write(w)) => batch.push(w),
                    Ok(WriterMsg::Rotate(reply)) => {
                        pending_rotate = Some(reply);
                        break;
                    }
                    Err(_) => break,
                }
            }
        }

        // Optional group-commit linger: hold the fsync briefly so more
        // concurrently in-flight writes can join this batch. Bounded by both the
        // window and a hard batch cap so a steady flood can't defer a commit
        // indefinitely. A rotate ends the linger (the boundary must be exact).
        if pending_rotate.is_none() && !linger.is_zero() && batch.len() < MAX_GROUP_BATCH {
            let deadline = Instant::now() + linger;
            while batch.len() < MAX_GROUP_BATCH {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                match rx.recv_timeout(remaining) {
                    Ok(WriterMsg::Write(w)) => batch.push(w),
                    Ok(WriterMsg::Rotate(reply)) => {
                        pending_rotate = Some(reply);
                        break;
                    }
                    Err(RecvTimeoutError::Timeout) => break,
                    // All senders gone: flush what we have; the outer `recv`
                    // will then observe the disconnect and exit.
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        }

        if !batch.is_empty() {
            let mut buf = Vec::new();
            let mut events: u64 = 0;
            let num_partitions = seg.shared().partitions();
            let mut deltas = vec![0u64; num_partitions];
            for req in &batch {
                buf.extend_from_slice(&req.bytes);
                events += req.events as u64;
                if num_partitions > 0 && (req.partition as usize) < num_partitions {
                    deltas[req.partition as usize] += req.events as u64;
                }
            }

            let fsync_start = Instant::now();
            if let Err(e) = seg.write_all(&buf, events).and_then(|()| seg.fsync()) {
                tracing::error!(
                    "journal write failed: {e}; aborting to avoid serving non-durable state"
                );
                std::process::abort();
            }
            // Advance per-partition cumulative counts BEFORE any seal, so the
            // sealed boundary's per-partition end matches the bytes just written
            // (no-op on the legacy/single-partition path).
            if num_partitions > 0 {
                seg.shared().add_partition_events(&deltas);
            }
            crate::metrics::record_commit(batch.len(), fsync_start.elapsed(), buf.len());
            crate::metrics::inflight_sub(batch.len());
            crate::metrics::journal_inflight_sub(buf.len());

            for req in batch {
                // The receiver is gone for fire-and-forget writes (the background
                // tick and startup seeding never await their commit); that's fine.
                let _ = req.ack.send(());
                // Forward to the read-model exporter shard for this write's
                // partition in log (fsync) order. `None` events_arc means this
                // journal forwards via `persist()` instead (single-partition /
                // in-memory / legacy). The serialized size (`req.bytes.len()`) is
                // the exporter-queue byte estimate for backpressure accounting.
                let export_bytes = req.bytes.len().min(u32::MAX as usize) as u32;
                forward_shared_export(&exporter, req.partition, req.events_arc, export_bytes);
            }

            // Size-based segment seal (no-op in the legacy single-file path,
            // whose threshold is infinite).
            if let Err(e) = seg.maybe_seal() {
                tracing::error!("journal segment seal failed: {e}; aborting");
                std::process::abort();
            }
        }

        // A rotate seals the active segment at the exact boundary AFTER the
        // preceding writes are durable, then reports it to the snapshot caller.
        if let Some(reply) = pending_rotate {
            match seg.seal() {
                Ok(info) => {
                    let _ = reply.send(info);
                }
                Err(e) => {
                    tracing::error!("journal rotate failed: {e}; aborting");
                    std::process::abort();
                }
            }
        }
        crate::metrics::record_writer_cycle(idle, busy_start.elapsed());
    }
}

/// The **async-durability** writer loop. Like [`writer_loop`] it group-commits a
/// batch with a single `write_all`, but it **acknowledges callers immediately
/// after the write reaches the OS page cache** and defers `fsync` to an
/// amortized cadence (every [`AsyncFlush::interval`] or
/// [`AsyncFlush::max_bytes`], whichever first). This removes the fsync media
/// barrier from every commit's critical path and lets fsyncs coalesce across
/// many batches, trading a bounded power-loss window for throughput. A process
/// crash loses nothing — the appended bytes survive in the page cache / file and
/// are replayed on restart; only an OS crash / power loss can lose the unfsynced
/// tail. When the producer goes quiet the loop wakes on a bounded
/// `recv_timeout` to flush that tail, so the window is always bounded by
/// `interval`. Write/fsync errors abort, identical to the sync path.
fn writer_loop_async(
    mut seg: ActiveSegment,
    rx: Receiver<WriterMsg>,
    linger: Duration,
    flush: AsyncFlush,
    exporter: Option<ExporterCell>,
) {
    let mut last_fsync = Instant::now();
    let mut unsynced_bytes: usize = 0;
    let mut unsynced_writes: usize = 0;

    // Force a durability barrier for everything written since the last fsync.
    fn do_fsync(
        seg: &mut ActiveSegment,
        bytes: &mut usize,
        writes: &mut usize,
        last: &mut Instant,
    ) {
        if *bytes == 0 {
            return;
        }
        let fsync_start = Instant::now();
        if let Err(e) = seg.fsync() {
            tracing::error!(
                "journal fsync failed: {e}; aborting to avoid serving non-durable state"
            );
            std::process::abort();
        }
        crate::metrics::record_fsync(*writes, fsync_start.elapsed());
        *bytes = 0;
        *writes = 0;
        *last = Instant::now();
    }

    loop {
        // Wait for the next message. While an unfsynced tail is outstanding,
        // bound the wait so a quiet producer can't leave it unflushed past the
        // configured interval.
        let idle_start = Instant::now();
        let first = if unsynced_bytes > 0 {
            let budget = flush
                .interval
                .checked_sub(last_fsync.elapsed())
                .unwrap_or_default();
            match rx.recv_timeout(budget) {
                Ok(msg) => msg,
                Err(RecvTimeoutError::Timeout) => {
                    do_fsync(
                        &mut seg,
                        &mut unsynced_bytes,
                        &mut unsynced_writes,
                        &mut last_fsync,
                    );
                    continue;
                }
                Err(RecvTimeoutError::Disconnected) => break,
            }
        } else {
            match rx.recv() {
                Ok(msg) => msg,
                Err(_) => break,
            }
        };
        let idle = idle_start.elapsed();
        let busy_start = Instant::now();

        let mut batch: Vec<WriteRequest> = Vec::new();
        let mut pending_rotate: Option<oneshot::Sender<crate::seglog::SealInfo>> = None;
        match first {
            WriterMsg::Write(w) => batch.push(w),
            WriterMsg::Rotate(reply) => pending_rotate = Some(reply),
        }

        if pending_rotate.is_none() {
            while batch.len() < MAX_GROUP_BATCH {
                match rx.try_recv() {
                    Ok(WriterMsg::Write(w)) => batch.push(w),
                    Ok(WriterMsg::Rotate(reply)) => {
                        pending_rotate = Some(reply);
                        break;
                    }
                    Err(_) => break,
                }
            }
        }
        // Linger still fattens the *write* batch (fewer write_all syscalls), even
        // though it no longer gates an fsync on the caller's behalf.
        if pending_rotate.is_none() && !linger.is_zero() && batch.len() < MAX_GROUP_BATCH {
            let deadline = Instant::now() + linger;
            while batch.len() < MAX_GROUP_BATCH {
                let Some(remaining) = deadline.checked_duration_since(Instant::now()) else {
                    break;
                };
                match rx.recv_timeout(remaining) {
                    Ok(WriterMsg::Write(w)) => batch.push(w),
                    Ok(WriterMsg::Rotate(reply)) => {
                        pending_rotate = Some(reply);
                        break;
                    }
                    Err(RecvTimeoutError::Timeout) => break,
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        }

        if !batch.is_empty() {
            let mut buf = Vec::new();
            let mut events: u64 = 0;
            let num_partitions = seg.shared().partitions();
            let mut deltas = vec![0u64; num_partitions];
            for req in &batch {
                buf.extend_from_slice(&req.bytes);
                events += req.events as u64;
                if num_partitions > 0 && (req.partition as usize) < num_partitions {
                    deltas[req.partition as usize] += req.events as u64;
                }
            }
            if let Err(e) = seg.write_all(&buf, events) {
                tracing::error!(
                    "journal write failed: {e}; aborting to avoid serving non-durable state"
                );
                std::process::abort();
            }
            if num_partitions > 0 {
                seg.shared().add_partition_events(&deltas);
            }
            unsynced_bytes += buf.len();
            unsynced_writes += batch.len();
            crate::metrics::record_write_batch(batch.len(), buf.len());

            // Acknowledge now: the write is in the page cache and will survive a
            // process restart; the amortized fsync below upgrades it to
            // power-loss-durable.
            crate::metrics::inflight_sub(batch.len());
            crate::metrics::journal_inflight_sub(buf.len());
            for req in batch {
                let _ = req.ack.send(());
                let export_bytes = req.bytes.len().min(u32::MAX as usize) as u32;
                forward_shared_export(&exporter, req.partition, req.events_arc, export_bytes);
            }

            if unsynced_bytes >= flush.max_bytes || last_fsync.elapsed() >= flush.interval {
                do_fsync(
                    &mut seg,
                    &mut unsynced_bytes,
                    &mut unsynced_writes,
                    &mut last_fsync,
                );
            }

            // A size-based seal flushes the segment it closes (seal fsyncs), so
            // the unsynced tail is now durable; reset the window.
            match seg.maybe_seal() {
                Ok(Some(_)) => {
                    unsynced_bytes = 0;
                    unsynced_writes = 0;
                    last_fsync = Instant::now();
                }
                Ok(None) => {}
                Err(e) => {
                    tracing::error!("journal segment seal failed: {e}; aborting");
                    std::process::abort();
                }
            }
        }

        // A rotate seals the active segment at the exact boundary. Sealing
        // fsyncs the closed segment, so the unsynced tail (now in that segment)
        // is durable; reset the window.
        if let Some(reply) = pending_rotate {
            match seg.seal() {
                Ok(info) => {
                    unsynced_bytes = 0;
                    unsynced_writes = 0;
                    last_fsync = Instant::now();
                    let _ = reply.send(info);
                }
                Err(e) => {
                    tracing::error!("journal rotate failed: {e}; aborting");
                    std::process::abort();
                }
            }
        }
        crate::metrics::record_writer_cycle(idle, busy_start.elapsed());
    }

    // Drain on shutdown: fsync whatever tail was acked but not yet synced.
    do_fsync(
        &mut seg,
        &mut unsynced_bytes,
        &mut unsynced_writes,
        &mut last_fsync,
    );
}

/// Group-commit linger window from `NANOBPMN_JOURNAL_LINGER_US` (microseconds).
/// Default 0 = off (drain-only group commit, original behaviour). A small value
/// (e.g. 200–2000µs) coalesces fsyncs on latency-bound workloads. Clamped to a
/// 50ms ceiling so a fat-fingered value can't stall durability.
fn journal_linger_from_env() -> Duration {
    const MAX_US: u64 = 50_000;
    match std::env::var("NANOBPMN_JOURNAL_LINGER_US") {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(us) => Duration::from_micros(us.min(MAX_US)),
            Err(_) => Duration::ZERO,
        },
        Err(_) => Duration::ZERO,
    }
}

/// Wall-clock milliseconds since the Unix epoch, for stamping the timestamped
/// events a host-injected command raises (e.g. the #1384 `VariablesLost`
/// incident) when no upstream command supplied a clock reading. A clock before
/// the epoch surfaces as `0`.
fn now_millis() -> u64 {
    std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

/// Durability mode for the journal writer.
///
/// - [`Sync`](DurabilityMode::Sync) (default): a write is acknowledged only
///   after it has been `fsync`ed. The strongest contract — anything the server
///   returns `200` for survives power loss — at the cost of putting the ~4 ms
///   media barrier on every commit's critical path.
/// - [`Async`](DurabilityMode::Async): a write is acknowledged once it is in the
///   OS page cache (after `write_all`), and `fsync` is amortized onto a periodic
///   cadence in the background. This takes the fsync latency off the caller's
///   critical path and lets fsyncs coalesce far more aggressively (the writer no
///   longer blocks a commit per fsync), at the cost of a weaker durability
///   window: a **process** crash loses nothing (the page cache, hence the log
///   file, survives and is replayed on restart), but an **OS crash or power
///   loss** can lose the unfsynced tail (bounded by the flush interval/bytes).
///   This mirrors Zeebe's async-exporter model: ack from the in-memory/appended
///   log, guarantee durability by journal replay on restart.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DurabilityMode {
    Sync,
    Async,
}

/// Async-durability flush policy: `fsync` fires when either the bytes written
/// since the last fsync reach `max_bytes`, or `interval` elapses (whichever
/// comes first). Tunable via `NANOBPMN_ASYNC_FLUSH_MS` / `NANOBPMN_ASYNC_FLUSH_BYTES`.
#[derive(Clone, Copy)]
struct AsyncFlush {
    interval: Duration,
    max_bytes: usize,
}

/// Durability mode from `NANOBPMN_DURABILITY` (`sync` | `async`). Defaults to
/// `sync` so the strong fsync-before-ack contract — and every existing test —
/// is unchanged unless async is explicitly opted into.
fn durability_mode_from_env() -> DurabilityMode {
    match std::env::var("NANOBPMN_DURABILITY") {
        Ok(v) if v.trim().eq_ignore_ascii_case("async") => DurabilityMode::Async,
        _ => DurabilityMode::Sync,
    }
}

/// Async flush policy from env. `NANOBPMN_ASYNC_FLUSH_MS` (default 10, clamped to
/// 1s) bounds the unfsynced time window; `NANOBPMN_ASYNC_FLUSH_BYTES` (default
/// 8 MiB) bounds the unfsynced byte window. Either trigger forces an fsync.
fn async_flush_from_env() -> AsyncFlush {
    let interval = match std::env::var("NANOBPMN_ASYNC_FLUSH_MS") {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms.clamp(1, 1000)),
            Err(_) => Duration::from_millis(10),
        },
        Err(_) => Duration::from_millis(10),
    };
    let max_bytes = match std::env::var("NANOBPMN_ASYNC_FLUSH_BYTES") {
        Ok(v) => v.trim().parse::<usize>().unwrap_or(8 << 20).max(4096),
        Err(_) => 8 << 20,
    };
    AsyncFlush {
        interval,
        max_bytes,
    }
}

/// Spawns the journal writer thread for `seg`/`rx`, selecting the sync or async
/// commit loop from `NANOBPMN_DURABILITY`. Shared by [`SharedWriter::open`],
/// [`Journal::open_partition`] and the segmented path so all honour the same
/// durability configuration.
fn spawn_writer(
    seg: ActiveSegment,
    rx: Receiver<WriterMsg>,
    exporter: Option<ExporterCell>,
) -> io::Result<JoinHandle<()>> {
    let linger = journal_linger_from_env();
    let mode = durability_mode_from_env();
    let flush = async_flush_from_env();
    thread::Builder::new()
        .name("nanobpmn-journal-writer".into())
        .spawn(move || match mode {
            DurabilityMode::Sync => writer_loop(seg, rx, linger, exporter),
            DurabilityMode::Async => writer_loop_async(seg, rx, linger, flush, exporter),
        })
}

/// file+thread. Because every partition's in-flight commands land in the *same*
/// group-commit batch, a single `fsync` drains them all — the batch coalesces
/// across the whole node's write concurrency.
///
/// This is the fix for **per-partition fsync fragmentation**: with one journal
/// file per partition, each writer only ever sees ~1/N of the node's concurrent
/// writes, so its group-commit batches are N× smaller and it must `fsync` N×
/// more often (and the disk's concurrent-fsync ceiling is hit sooner). Funneling
/// every partition through one writer makes the fsync rate independent of the
/// partition count while keeping the partitions fully independent for
/// *processing* (each still has its own single-writer engine actor and key
/// namespace; events are tagged by partition via their keys, so the shared log
/// is demultiplexed back to the owning partition on replay).
///
/// The writer thread is detached (its [`JoinHandle`] is dropped): like the
/// engine threads, durability never depends on a clean shutdown — a command is
/// acked only after its [`Commit`] resolves, i.e. after the write is fsynced —
/// so the thread can simply be reclaimed by the OS on exit. It stays alive as
/// long as any partition journal (holding a cloned sender) is alive.
pub struct SharedWriter {
    tx: Sender<WriterMsg>,
    /// Segmented seal/boundary state when this is the bounded-disk
    /// multi-partition writer; `None` for the legacy single-file shared log.
    seg: Option<Arc<SegShared>>,
    /// Exporter cell shared with the writer thread: the read-model exporter
    /// sender is dropped in here (via a shared-backed [`Journal::set_exporter`])
    /// so the writer forwards events in fsync/log order. `None` for the legacy
    /// path (which forwards from `persist()`).
    exporter: Option<ExporterCell>,
}

impl SharedWriter {
    /// Opens (creating if absent, positioned to append) the shared log at `path`
    /// and spawns its group-commit writer thread, reading the same
    /// `NANOBPMN_JOURNAL_LINGER_US` linger window as a per-partition journal.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        let shared = SegShared::legacy(path.as_ref().to_path_buf());
        let seg = ActiveSegment::open(shared)?;
        let (tx, rx) = mpsc::channel::<WriterMsg>();
        spawn_writer(seg, rx, None)?;
        Ok(Self {
            tx,
            seg: None,
            exporter: None,
        })
    }

    /// Opens (or recovers) the bounded-disk **segmented multi-partition** shared
    /// log rooted at directory `dir`, sized to `num_partitions`. Rebuilds every
    /// owned partition's engine from the combined snapshot + surviving tail (see
    /// [`crate::seglog::recover_multi`]) and spawns the segment-aware writer with
    /// an exporter cell so the read model is fed a single global prefix in log
    /// order. Returns the writer plus the per-partition recovery.
    pub fn open_segmented(
        dir: impl AsRef<Path>,
        owned: &[u64],
        num_partitions: u64,
        varstore: Option<&VarStore>,
    ) -> io::Result<(Self, crate::seglog::MultiSegRecovery)> {
        let recovery =
            crate::seglog::recover_multi(dir.as_ref(), owned, num_partitions as usize, varstore)?;
        let shared = Arc::clone(&recovery.shared);
        let seg_active = ActiveSegment::open(Arc::clone(&shared))?;
        let (tx, rx) = mpsc::channel::<WriterMsg>();
        let exporter: ExporterCell = Arc::new(Mutex::new(HashMap::new()));
        spawn_writer(seg_active, rx, Some(Arc::clone(&exporter)))?;
        Ok((
            Self {
                tx,
                seg: Some(shared),
                exporter: Some(exporter),
            },
            recovery,
        ))
    }

    /// A fresh sender into the shared writer, for one partition's [`Journal`].
    /// The writer thread lives until every such sender is dropped.
    fn sender(&self) -> Sender<WriterMsg> {
        self.tx.clone()
    }

    /// The segmented seal state, if this is the bounded-disk multi-partition
    /// writer (used to drive per-partition snapshot/rotate and compaction).
    pub fn seg_shared(&self) -> Option<Arc<SegShared>> {
        self.seg.clone()
    }
}

#[cfg(test)]
thread_local! {
    /// Test-only crash injection for [`Journal::checkpoint_base`]: fail right
    /// after the rebase floor is published, before any authoritative state is
    /// touched (#1405).
    pub(crate) static FAIL_AFTER_FLOOR: std::cell::Cell<bool> = const { std::cell::Cell::new(false) };
}

impl Journal {
    /// A non-persistent journal: the engine runs purely in memory and nothing is
    /// written. Used for tests and ephemeral runs.
    pub fn in_memory() -> Self {
        Self::in_memory_partition(0)
    }

    /// Like [`Journal::in_memory`] but the engine mints keys in `partition_id`'s
    /// namespace (see [`nanobpmn_engine_core::partition_of`]).
    pub fn in_memory_partition(partition_id: u64) -> Self {
        Self {
            engine: Engine::with_partition(partition_id),
            partition_id,
            writer: None,
            writer_thread: None,
            seg: None,
            exporter: None,
            shared_exporter: None,
            fresh: true,
            spill: None,
            cold: None,
            varstore: None,
            quarantined: Mutex::default(),
            offheap_read_failures: AtomicU64::new(0),
            snapshot_blockers: SnapshotBlockers::default(),
            replicated_apply: false,
            activation_cursor: HashMap::new(),
            raft_entry: None,
            fenced: false,
            #[cfg(test)]
            resident_child_scans: 0,
            spill_check_skip: 0,
            cluster_variables: ClusterVariables::default(),
        }
    }

    /// Like [`Journal::in_memory_partition`] but seeds the engine by replaying
    /// `events` (non-persistent). Used to rebuild a volatile state machine from a
    /// snapshot body (a serialized event history) — the same replay path as crash
    /// recovery, minus the writer.
    pub fn in_memory_from_events(partition_id: u64, events: Vec<Event>) -> Self {
        let engine = if events.is_empty() {
            Engine::with_partition(partition_id)
        } else {
            Engine::replay_partition(partition_id, events)
        };
        Self {
            engine,
            partition_id,
            writer: None,
            writer_thread: None,
            seg: None,
            exporter: None,
            shared_exporter: None,
            fresh: false,
            spill: None,
            cold: None,
            varstore: None,
            quarantined: Mutex::default(),
            offheap_read_failures: AtomicU64::new(0),
            snapshot_blockers: SnapshotBlockers::default(),
            replicated_apply: false,
            activation_cursor: HashMap::new(),
            raft_entry: None,
            fenced: false,
            #[cfg(test)]
            resident_child_scans: 0,
            spill_check_skip: 0,
            cluster_variables: ClusterVariables::default(),
        }
    }

    /// Adopt the engine state captured by a Raft snapshot **in place**, preserving
    /// this journal's read-model exporter wiring, partition id, authoritative var
    /// store and spill tiers.
    ///
    /// Used by the Raft install-snapshot ([`crate::raft`] `install_snapshot`) and
    /// boot-restore (`restore_from_current_snapshot`) paths. A snapshot install
    /// must NOT tear down the journal's exporter binding: an owned partition that
    /// catches up via a snapshot install (e.g. a returning owner reclaiming its
    /// partitions after a bounce) would otherwise silently stop projecting to the
    /// read model, so its own completed instances would never be eviction-reaped
    /// and would pile up unbounded in hot state. Owned partitions rely on the
    /// exporter — not self-eviction (`evict_terminal`) — to shed terminal
    /// instances, so dropping the exporter leaks the whole reclaimed backlog.
    ///
    /// The received snapshot is a full, self-contained
    /// [`nanobpmn_engine_core::EngineSnapshot`] (control state + variables), so
    /// the engine is fully resident afterward; the preserved var store / spill
    /// tiers only participate in later spill write-through / rehydrate.
    ///
    /// The snapshot's `State` is cloned wholesale by [`Engine::snapshot`], so it
    /// carries the source's resident **terminal** (Completed/Terminated) shells —
    /// the exporter-lag / retirement backlog the leader held at build time. Those
    /// instances are done and already durable in the read model at their source;
    /// they never flow through THIS node's exporter (they arrived by snapshot, not
    /// as replayed events), so nothing would ever evict them and they would pin
    /// hot state on a reclaiming owner. Shed them here so the installed hot state
    /// holds only in-flight instances — the same invariant the boot replay
    /// establishes with its post-catch-up `evict_completed` sweep.
    pub fn restore_engine_from_snapshot(&mut self, snapshot: nanobpmn_engine_core::EngineSnapshot) {
        // Config the snapshot body does not carry, re-applied onto the new engine.
        let lenient = self.engine.lenient_completion();
        self.engine = Engine::from_snapshot(snapshot);
        self.engine.set_lenient_completion(lenient);
        // Cluster variables are host-injected external config, absent from the
        // snapshot body: re-link the fresh engine to the live shared handle.
        self.engine
            .set_cluster_variables(self.cluster_variables.clone());
        // If lean-snapshot mode is wired, re-enable dirty-var tracking on the
        // fresh engine and re-seed the dirty set so the next checkpoint rewrites
        // the full resident variable state into the authoritative store
        // (idempotent — mirrors `set_varstore`).
        if self.varstore.is_some() {
            self.engine.set_track_dirty_vars(true);
            self.engine.mark_all_dirty();
        }
        // Drop the snapshot's terminal shells: hot state keeps only in-flight work.
        self.engine.evict_completed();
        // The received snapshot is self-contained: every cold (wholly off-heap)
        // instance was folded back in as resident (`fold_cold_into`), so this
        // receiver's own pre-existing cold routing index is now entirely stale.
        // Clear it — otherwise a later command for a key this node happened to
        // hold cold would route through `ensure_resident_for_command` →
        // `rehydrate_cold` and clobber the freshly installed resident state with
        // the superseded cold row (#1331). The now-unreferenced on-disk cold
        // rows are reaped by the post-recovery GC (`gc_offheap_orphans`, #1338).
        if let Some(cold) = self.cold.as_mut() {
            cold.index = ColdIndex::default();
        }
        self.fresh = false;
    }

    /// Replaces this journal's engine with one rebuilt from authoritative
    /// durable state, clearing the #1405 abort fence (#1473). Used by the Raft
    /// partition-rebuild path when the predecessor's journal was fenced by an
    /// aborted Raft entry: the live engine may hold the entry's partial,
    /// non-durable effects, so the whole engine is swapped for the
    /// disk-reconstructed one rather than merely clearing the fence.
    ///
    /// Like [`restore_engine_from_snapshot`](Self::restore_engine_from_snapshot),
    /// this preserves everything the journal carries that is NOT engine state:
    /// the writer/exporter/seg wiring, the spill tiers, the var store, and the
    /// host-injected cluster variables are all re-linked onto the fresh engine.
    /// The cold routing index is cleared (the rebuilt engine is fully resident
    /// from its durable replay; any stale cold rows are reaped by the
    /// post-recovery GC).
    pub fn replace_engine(&mut self, engine: Engine) {
        let lenient = self.engine.lenient_completion();
        self.engine = engine;
        self.engine.set_lenient_completion(lenient);
        self.engine
            .set_cluster_variables(self.cluster_variables.clone());
        if self.varstore.is_some() {
            self.engine.set_track_dirty_vars(true);
            self.engine.mark_all_dirty();
        }
        if let Some(cold) = self.cold.as_mut() {
            cold.index = ColdIndex::default();
        }
        // Evict the rebuilt terminal shells (#1473): a durable replay resurrects
        // every completed instance whose terminal event survived, but those
        // events are not re-exported here, so the exporter never drives another
        // eviction for them — without this sweep the rebuilt engine pins its
        // whole completed history resident indefinitely (the boot path runs the
        // same post-catch-up `evict_completed` for the same reason).
        self.engine.evict_completed();
        // The rebuild IS the durable state: the fence and any armed (aborted)
        // raft-entry buffer are predecessor state that must not outlive it.
        self.raft_entry = None;
        self.fenced = false;
        self.fresh = false;
    }

    /// Reads and deserializes every event from the journal log at `path` (an
    /// empty vec if the file does not exist). Shared by [`Journal::open`] (to
    /// replay into the engine) and the boot-time read-model catch-up.
    pub fn read_events(path: impl AsRef<Path>) -> io::Result<Vec<Event>> {
        // Frame-aware (#1405): the single-file journal can carry atomic Raft
        // entry frames, and shares the segment reader's torn-tail contract.
        crate::seglog::read_segment_events(path.as_ref())
    }

    /// Opens (creating if absent) the journal at `path`, replaying any existing
    /// log to reconstruct engine state, then spawns the background writer thread
    /// positioned to append.
    pub fn open(path: impl AsRef<Path>) -> io::Result<Self> {
        Self::open_partition(path, 0)
    }

    /// Like [`Journal::open`] but the engine owns `partition_id`'s key namespace.
    /// Replay only advances this partition's local key counter (foreign keys, e.g.
    /// a deployment replicated from partition 0, are applied to state but do not
    /// advance the counter — see [`Engine::replay_partition`]).
    pub fn open_partition(path: impl AsRef<Path>, partition_id: u64) -> io::Result<Self> {
        let path = path.as_ref();
        let mut engine = Engine::with_partition(partition_id);
        let mut fresh = true;

        let events = Self::read_events(path)?;
        if !events.is_empty() {
            engine = Engine::replay_partition(partition_id, events);
            fresh = false;
        }

        let shared = SegShared::legacy(path.to_path_buf());
        let seg_active = ActiveSegment::open(shared)?;
        let (tx, rx) = mpsc::channel::<WriterMsg>();
        let writer_thread = spawn_writer(seg_active, rx, None)?;

        Ok(Self {
            engine,
            partition_id,
            writer: Some(tx),
            writer_thread: Some(writer_thread),
            seg: None,
            exporter: None,
            shared_exporter: None,
            fresh,
            spill: None,
            cold: None,
            varstore: None,
            quarantined: Mutex::default(),
            offheap_read_failures: AtomicU64::new(0),
            snapshot_blockers: SnapshotBlockers::default(),
            replicated_apply: false,
            activation_cursor: HashMap::new(),
            raft_entry: None,
            fenced: false,
            #[cfg(test)]
            resident_child_scans: 0,
            spill_check_skip: 0,
            cluster_variables: ClusterVariables::default(),
        })
    }

    /// Opens (or recovers) a **segmented** journal rooted at directory `dir` —
    /// the bounded-disk single-partition path. Loads the latest snapshot, replays
    /// only the events it did not cover, and spawns the segment-aware writer so
    /// the active segment continues from the recovered boundary. The returned
    /// surviving event list + `first_index` let the caller catch the read model
    /// up (events compacted before `first_index` are already in the read model).
    pub fn open_segmented(dir: impl AsRef<Path>) -> io::Result<(Self, crate::seglog::SegRecovery)> {
        let (engine, recovery) = crate::seglog::recover(dir.as_ref())?;
        let fresh = recovery.fresh;
        let shared = Arc::clone(&recovery.shared);
        let seg_active = ActiveSegment::open(Arc::clone(&shared))?;
        let (tx, rx) = mpsc::channel::<WriterMsg>();
        let writer_thread = spawn_writer(seg_active, rx, None)?;

        let journal = Self {
            engine,
            partition_id: 0,
            writer: Some(tx),
            writer_thread: Some(writer_thread),
            seg: Some(shared),
            exporter: None,
            shared_exporter: None,
            fresh,
            spill: None,
            cold: None,
            varstore: None,
            quarantined: Mutex::default(),
            offheap_read_failures: AtomicU64::new(0),
            snapshot_blockers: SnapshotBlockers::default(),
            replicated_apply: false,
            activation_cursor: HashMap::new(),
            raft_entry: None,
            fenced: false,
            #[cfg(test)]
            resident_child_scans: 0,
            spill_check_skip: 0,
            cluster_variables: ClusterVariables::default(),
        };
        Ok((journal, recovery))
    }

    /// Builds a partition journal that persists through a [`SharedWriter`] (one
    /// fsync stream shared across every partition) instead of a private
    /// file+thread. `events` are *this* partition's events, already split out of
    /// the shared log by the caller (which reads the single log once and routes
    /// each event to `partition_of(key)`); they reconstruct the engine exactly as
    /// [`Journal::open_partition`] would. The journal owns a cloned sender into
    /// the shared writer, so the writer thread stays alive as long as it does.
    pub fn from_events_shared(
        partition_id: u64,
        events: Vec<Event>,
        shared: &SharedWriter,
    ) -> Self {
        let fresh = events.is_empty();
        let engine = if fresh {
            Engine::with_partition(partition_id)
        } else {
            Engine::replay_partition(partition_id, events)
        };
        Self {
            engine,
            partition_id,
            writer: Some(shared.sender()),
            writer_thread: None,
            seg: shared.seg.clone(),
            exporter: None,
            shared_exporter: shared.exporter.clone(),
            fresh,
            spill: None,
            cold: None,
            varstore: None,
            quarantined: Mutex::default(),
            offheap_read_failures: AtomicU64::new(0),
            snapshot_blockers: SnapshotBlockers::default(),
            replicated_apply: false,
            activation_cursor: HashMap::new(),
            raft_entry: None,
            fenced: false,
            #[cfg(test)]
            resident_child_scans: 0,
            spill_check_skip: 0,
            cluster_variables: ClusterVariables::default(),
        }
    }

    /// Builds a partition journal that persists through a segmented
    /// [`SharedWriter`] from an already-reconstructed `engine` (the bounded-disk
    /// multi-partition recovery path). Unlike [`Journal::from_events_shared`],
    /// which replays this partition's full event history, the engine here was
    /// rebuilt from a combined snapshot + surviving-tail replay by
    /// [`crate::seglog::recover_multi`]. Inherits the writer's segmented seal
    /// state and exporter cell so snapshots/compaction and read-model forwarding
    /// work for this partition.
    pub fn from_engine_shared(
        partition_id: u64,
        engine: Engine,
        fresh: bool,
        shared: &SharedWriter,
    ) -> Self {
        Self {
            engine,
            partition_id,
            writer: Some(shared.sender()),
            writer_thread: None,
            seg: shared.seg.clone(),
            exporter: None,
            shared_exporter: shared.exporter.clone(),
            fresh,
            spill: None,
            cold: None,
            varstore: None,
            quarantined: Mutex::default(),
            offheap_read_failures: AtomicU64::new(0),
            snapshot_blockers: SnapshotBlockers::default(),
            replicated_apply: false,
            activation_cursor: HashMap::new(),
            raft_entry: None,
            fenced: false,
            #[cfg(test)]
            resident_child_scans: 0,
            spill_check_skip: 0,
            cluster_variables: ClusterVariables::default(),
        }
    }

    /// Installs an already-minted deployment (the [`Event`]s from a `Deploy`
    /// command processed on another partition) into this partition's engine without minting new keys, so every partition shares the identical
    /// process definition under the identical key. Not journaled here: a
    /// multi-partition host re-derives the replication on restart from the
    /// deployment partition's log (which is the single durable record of the
    /// deployment). See [`Engine::install_deployment`].
    pub fn install_deployment(&mut self, events: &[Event]) {
        self.engine.install_deployment(events);
    }

    /// Installs a deployment seed onto a freshly rebuilt engine, keeping every
    /// historical version and never regressing the latest-by-id index
    /// (version-guarded). Used by the Raft rebuild path (#1473) to reseed the
    /// canonical deployments onto an engine reconstructed from durable state —
    /// definitions journaled under a different partition are not in `p`'s own
    /// replay, so without this the rebuilt partition's creates fail with
    /// `ProcessNotFound`. See [`Engine::install_deployment_if_newer`].
    pub fn install_deployment_if_newer(&mut self, events: &[Event]) {
        self.engine.install_deployment_if_newer(events);
    }

    /// The authoritative lean-snapshot variable store wired onto this journal,
    /// if any (#1463). The Raft rebuild's durable recovery reads it so a lean
    /// (control-only) combined snapshot's variables are installed before the
    /// surviving tail replays (#1473).
    pub fn varstore(&self) -> Option<Arc<VarStore>> {
        self.varstore.clone()
    }

    /// Durably installs a deployment replicated from another node: registers the
    /// definition(s) (no key minting, no start subscriptions — `ProcessDeployed`
    /// events only) **and** journals them so they survive this node's
    /// independent restart. Unlike [`install_deployment`](Self::install_deployment)
    /// (in-memory only, re-derived on restart from the local deployment
    /// partition's log), a clustered peer owns no deployment partition, so it must
    /// record its own durable copy. The journaled `ProcessDeployed` events carry
    /// the deployment partition's keys; the restart demux replays every
    /// `ProcessDeployed` into every owned partition (definitions are partition-
    /// agnostic), so a single durable copy reconstructs the definition on all of
    /// this node's partitions. Returns a [`Commit`] the caller awaits before
    /// acknowledging the install.
    pub fn install_deployment_durable(&mut self, events: &[Event]) -> Commit {
        self.engine.install_deployment(events);
        let deployed: Vec<Event> = events
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    Event::ProcessDeployed { .. }
                        | Event::DecisionRequirementsDeployed { .. }
                        | Event::DecisionDeployed { .. }
                        | Event::FormDeployed { .. }
                        | Event::GenericResourceDeployed { .. }
                )
            })
            .cloned()
            .collect();
        self.persist(&Arc::new(deployed))
    }

    /// Wires the disk-backed variable spill. `budget` is the maximum number of
    /// resident instances allowed to hold their variables in hot RAM; once the
    /// active backlog exceeds it, each command that grows the backlog sheds the
    /// oldest instances' variables to `store` (rehydrated on job activation). Set
    /// before serving. A `budget` of 0 spills aggressively (keeps nothing extra
    /// resident); leaving the spill unset keeps the original all-resident
    /// behaviour.
    pub fn set_spill(&mut self, store: Arc<VarSpillStore>, budget: usize) {
        self.spill = Some(VarSpill {
            store,
            trigger: VarSpillTrigger::Budget(budget),
        });
    }

    /// Wires the authoritative durable variable store (lean-snapshot mode) and
    /// enables engine dirty-variable tracking so the periodic checkpoint can write
    /// just the delta. Under lean mode the snapshot carries only control state;
    /// variable spill write-through, job-activation rehydration and terminal
    /// forget all target this store instead of the destructive spill cache. Set
    /// before serving.
    /// Marks this journal as applied under replicated (Raft) state-machine
    /// application (#1384): command-path off-heap-orphan quarantines are then
    /// observed only (log + metric) instead of raising a non-deterministic
    /// durable incident.
    pub fn set_replicated_apply(&mut self, replicated: bool) {
        self.replicated_apply = replicated;
    }

    pub fn set_varstore(&mut self, store: Arc<VarStore>) {
        self.engine.set_track_dirty_vars(true);
        // Seed the dirty set with every currently-resident instance so the FIRST
        // lean checkpoint writes the full current variable state into the store.
        // This migrates a deployment booting from a pre-existing full snapshot
        // (var store empty) and is a harmless idempotent re-write on a normal
        // lean restart (recovery already installed the same maps).
        self.engine.mark_all_dirty();
        self.varstore = Some(store);
    }

    /// Whether lean-snapshot mode (authoritative durable var store) is wired.
    pub fn has_varstore(&self) -> bool {
        self.varstore.is_some()
    }

    /// Wires *adaptive* variable spill onto `store`. The oldest active-backlog
    /// variables are shed strictly on **measured memory** (see
    /// [`VarSpillTrigger::Adaptive`]):
    ///
    /// - below `high_water` with free RAM: never spill — full throughput;
    /// - resident `>= high_water`: reclaim to `low_water` — the OOM guard;
    /// - live system available memory below `reserve`: reclaim to `floor` (free
    ///   RAM contended — reclaim harder).
    ///
    /// The memory check runs on the periodic sweep
    /// ([`maybe_var_spill_pressure`](Journal::maybe_var_spill_pressure));
    /// `hard_cap` is a per-command instance-count backstop for a runaway between
    /// sweeps (0 disables it); `reserve == 0` disables the available-memory guard.
    /// Set before serving.
    pub fn set_var_spill_adaptive(
        &mut self,
        store: Arc<VarSpillStore>,
        floor: u64,
        high_water: u64,
        low_water: u64,
        reserve: u64,
        hard_cap: usize,
    ) {
        self.spill = Some(VarSpill {
            store,
            trigger: VarSpillTrigger::Adaptive {
                floor,
                high_water,
                low_water,
                reserve,
                hard_cap,
            },
        });
    }

    /// Wires cold spill onto the same disk-backed `store`. Once the engine's
    /// resident memory crosses `high_water` bytes, the periodic sweep
    /// ([`maybe_cold_spill`](Journal::maybe_cold_spill)) evicts whole dormant
    /// instances to the store — oldest first — until resident memory falls back
    /// under `low_water`, keeping only a slim routing index resident. A cold
    /// instance is rehydrated on demand the moment an event targets it. Set
    /// before serving; leaving it unset keeps every instance resident.
    pub fn set_cold_spill(&mut self, store: Arc<VarSpillStore>, high_water: u64, low_water: u64) {
        self.cold = Some(ColdSpill {
            store,
            index: ColdIndex::default(),
            high_water,
            low_water,
            shedding: false,
        });
    }

    /// How many instances are currently cold (off-heap). Zero when cold spill is
    /// unset or nothing is parked. Exposed for observability/tests.
    pub fn cold_count(&self) -> usize {
        self.cold.as_ref().map(|c| c.index.len()).unwrap_or(0)
    }

    /// Collects every live (non-terminal) instance key the engine holds for this
    /// partition — hot resident instances plus cold-spilled ones — into `out`.
    /// This is the authoritative set reconciliation compares the read model's
    /// `Active` rows against: any read row whose key is absent here corresponds to
    /// an instance the engine has already driven to a terminal state, so its read
    /// row is an orphan (its terminal event was never projected).
    ///
    /// The hot side is filtered to **non-terminal** instances: `Active`,
    /// `Suspended` and `Terminating` all retain their full runtime state (a
    /// suspended instance resumes exactly; a terminating one is still draining
    /// its `canceling` task listeners) and either can still carry
    /// `variables_spilled`, so both the read-model reconciler and the
    /// post-recovery off-heap GC ([#1338], reworked after #1467 excluded the
    /// two non-`Active` nonterminal states and reaped their still-referenced
    /// spill rows) must treat them as live. The engine normally evicts a
    /// terminal instance the moment it is reached, but a leader keeps terminal
    /// shells resident until its exporter drives eviction, and a node that
    /// reclaimed a partition can retain a large backlog of resident terminal
    /// shells whose completions it applied via raft catch-up (never
    /// exporter-evicted). Counting those resident-but-terminal shells as "live"
    /// would defeat reconciliation: an orphaned `Active` read row whose engine
    /// instance is resident-`Completed` would be treated as still live and never
    /// retired, pinning the active-backlog gauge forever (the post-bounce "stuck
    /// active instances" symptom). The cold side needs no filter — a terminal
    /// instance is `forget`-ten from the store on eviction, never spilled, so
    /// `cold.index` only ever holds dormant (non-terminal) instances.
    pub fn collect_live_instance_keys(&self, out: &mut std::collections::HashSet<Key>) {
        out.extend(
            self.state()
                .instances
                .values()
                .filter(|inst| {
                    matches!(
                        inst.state,
                        nanobpmn_engine_core::ProcessInstanceState::Active
                            | nanobpmn_engine_core::ProcessInstanceState::Suspended
                            | nanobpmn_engine_core::ProcessInstanceState::Terminating
                    )
                })
                .map(|inst| inst.key),
        );
        if let Some(cold) = self.cold.as_ref() {
            out.extend(cold.index.keys());
        }
    }

    /// The spill-reference set for the post-recovery GC ([#1338]): every
    /// non-terminal instance whose `variables_spilled` flag is still set. A
    /// legacy (pre-self-contained-snapshot) snapshot records a spilled instance
    /// as an empty placeholder, so after log compaction the spill row is the
    /// ONLY surviving copy of that instance's variables; the flag is what keeps
    /// it referenced. The filter spans every nonterminal state — `Active`,
    /// `Suspended` and `Terminating` alike can retain the flag (suspending or
    /// cancel-listener-deferring never rehydrates the payload) — matching
    /// [`collect_live_instance_keys`](Self::collect_live_instance_keys).
    pub fn gc_spill_reference_keys(&self) -> std::collections::HashSet<Key> {
        self.state()
            .instances
            .values()
            .filter(|inst| {
                inst.variables_spilled
                    && matches!(
                        inst.state,
                        nanobpmn_engine_core::ProcessInstanceState::Active
                            | nanobpmn_engine_core::ProcessInstanceState::Suspended
                            | nanobpmn_engine_core::ProcessInstanceState::Terminating
                    )
            })
            .map(|inst| inst.key)
            .collect()
    }

    /// Post-recovery garbage collection of orphaned cold/spill rows ([#1338])
    /// scoped to THIS partition's recovered engine. The node-level entry point
    /// is the server's post-bootstrap sweep, which drives
    /// [`gc_offheap_orphans_locked`](Self::gc_offheap_orphans_locked) once per
    /// hosted partition through its engine actor; this unfenced per-journal
    /// variant exists for tests that already hold the journal exclusively.
    /// Returns the number of orphaned rows reaped.
    pub fn gc_offheap_orphans(&self) -> u64 {
        let mut live = std::collections::HashSet::new();
        self.collect_live_instance_keys(&mut live);
        let spill_referenced = self.gc_spill_reference_keys();
        self.gc_offheap_orphans_with(&live, &spill_referenced)
    }

    /// Post-recovery garbage collection of orphaned cold/spill rows ([#1338])
    /// scoped to THIS partition's recovered engine, fenced against concurrent
    /// writes. The node-level entry point is the server's post-bootstrap sweep,
    /// which drives this once per hosted partition through the engine actor
    /// (`DeepthiHandle::with`): the closure holds the partition's single-writer
    /// thread for the whole sweep, so no create/complete/replicated apply can
    /// mint or spill a key between the live-set capture and the deletes — a
    /// detached sweep over fixed sets could reap an instance created after the
    /// capture (its spill row written, its key absent from the sets), leaving a
    /// spilled flag pointing at a missing row (the #1479 review finding). The
    /// store's paged commits release the connection lock between pages, so
    /// OTHER partitions' live traffic is not starved while this one sweeps.
    ///
    /// Runs AFTER recovery completes for this partition (under Raft: after
    /// `bootstrap_member` restored its durable snapshot — the server skips the
    /// sweep entirely when any hosted partition failed to recover), so the
    /// engine state is authoritative for liveness. Returns the number of
    /// orphaned rows reaped.
    pub fn gc_offheap_orphans_locked(&mut self) -> u64 {
        // Durability barrier (#1479 review): the actor fence orders the sweep
        // after every earlier command, but not after their journal WRITES. A
        // cancel applied in memory whose events are still queued on the writer
        // would drop the key from `live`, and a crash before that write lands
        // would restore the instance with its only spill row reaped. Block (we
        // are on the engine's dedicated OS thread, never an async runtime)
        // until every already-enqueued write is fsynced before capturing the
        // protection sets; the fence is still held, so nothing new enqueues.
        self.barrier().blocking_wait();
        let mut live = std::collections::HashSet::new();
        self.collect_live_instance_keys(&mut live);
        let spill_referenced = self.gc_spill_reference_keys();
        self.gc_offheap_orphans_with(&live, &spill_referenced)
    }

    /// The store-diffing core of the post-recovery GC ([#1338], reworked after
    /// the reverted #1467), split out so the fenced entry point
    /// ([`gc_offheap_orphans_locked`](Self::gc_offheap_orphans_locked)) and
    /// tests can drive it with an explicit live set. The sweep reaps only rows
    /// THIS partition minted (`gc_orphans` bounds the delete to
    /// `self.partition_id()`), so a partition's fenced pass can never delete a
    /// row its own single-writer fence does not cover — cross-partition rows
    /// are reaped when their own partition sweeps (the #1479 fence-vs-union
    /// finding: a per-partition sweep over the shared store with a
    /// store-wide delete would reap the other partitions' live rows, the #1467
    /// data-loss defect).
    ///
    /// **Durability/ordering contract.** `live` must be the authoritative
    /// live-instance set (every nonterminal instance — `Active`, `Suspended`,
    /// `Terminating` — hot or cold) and `spill_referenced` the set of live
    /// instances whose `variables_spilled` flag is still set, both taken AFTER
    /// recovery completes for every partition the store serves (under Raft:
    /// after bootstrap has hosted every replica and restored its durable
    /// snapshot, so a live instance restored later is never missed). At that
    /// point no command is mid-flight for a key the sweep would delete:
    ///
    /// - a **cold** row is reachable only through the routing index of a live
    ///   instance; a row whose key is absent from `live` is dead (its terminal
    ///   eviction `forget` was lost to a crash, or its routing index was never
    ///   persisted by a pre-self-contained-snapshot engine) and is reaped;
    /// - a **spill** row is reachable while its instance is live AND still
    ///   carries the `variables_spilled` flag (a legacy snapshot can leave that
    ///   flag pointing at the row — the only surviving copy of its variables);
    ///   a row whose instance is terminal/evicted is dead and is reaped. The
    ///   sweep conservatively retains every `live` key's spill row too (see
    ///   [`VarSpillStore::gc_orphans`]); it is not the mechanism that reclaims
    ///   live-but-no-longer-spilled rows.
    ///
    /// A SQLite failure is latched into the store's `forget_faults` (never
    /// silent) and surfaced in the log; already-committed pages stay deleted
    /// and the next boot retries the remainder.
    pub fn gc_offheap_orphans_with(
        &self,
        live: &std::collections::HashSet<Key>,
        spill_referenced: &std::collections::HashSet<Key>,
    ) -> u64 {
        let store = match self.spill_store() {
            Some(store) => store,
            None => return 0,
        };
        match store.gc_orphans(live, spill_referenced, self.partition_id()) {
            Ok(collected) => {
                if collected > 0 {
                    tracing::info!(
                        collected,
                        live = live.len(),
                        spill_referenced = spill_referenced.len(),
                        "post-recovery GC: reaped orphaned cold/spill rows (crash between \
                         row write and terminal forget, or a pre-self-contained-snapshot row)"
                    );
                }
                collected
            }
            Err(err) => {
                // The store already latched the typed error into `forget_faults`;
                // surface it here too so the boot log names the GC failure. The
                // next boot retries the un-deleted remainder.
                tracing::error!(
                    error = %err,
                    "post-recovery GC of orphaned cold/spill rows failed (latched for retry)"
                );
                0
            }
        }
    }

    /// Rehydrates the cold instance `key` back into hot state. A no-op (returns
    /// `false`) if cold spill is unset or `key` is not cold. Never destructive
    /// before the instance is safe (#1388): the snapshot is read
    /// non-destructively, restored into the engine, and only then is the routing
    /// entry dropped and the row forgotten.
    ///
    /// - **Unreadable** (SQLite/IO error or undecodable row): logged at ERROR and
    ///   counted ([`offheap_read_failures`](Journal::offheap_read_failures)); the
    ///   index entry and the row are KEPT, so the instance stays routable and the
    ///   next command/poll retries.
    /// - **Definitively absent** (readable store, no row): the instance is
    ///   already lost; quarantined via the #1383 orphan path (logged once,
    ///   listed in [`quarantined_orphans`](Journal::quarantined_orphans)) and
    ///   the stale index entry dropped so it cannot loop.
    fn rehydrate_cold(&mut self, key: Key) -> ColdRehydration {
        let store = match self.cold.as_ref() {
            Some(cold) if cold.index.contains(key) => Arc::clone(&cold.store),
            _ => return ColdRehydration::NotCold,
        };
        match store.try_get_cold(key) {
            Ok(Some(snapshot)) => {
                self.engine.rehydrate_instance(snapshot);
                if let Some(cold) = self.cold.as_mut() {
                    cold.index.remove(key);
                }
                // The instance is resident; the row is now dead. A failed delete
                // is latched in the store's forget faults (never silent) and the
                // row is reaped on a later forget/retire.
                if let Err(err) = store.forget(&[key]) {
                    tracing::error!(
                        instance_key = key,
                        error = %err,
                        "cold rehydration: instance restored but its cold row could not \
                         be forgotten (latched for retry)"
                    );
                }
                ColdRehydration::Restored
            }
            Ok(None) => {
                if let Some(cold) = self.cold.as_mut() {
                    cold.index.remove(key);
                }
                self.note_orphan(
                    key,
                    "cold rehydration: cold instance has no row in the spill store; it is \
                     lost — quarantined (routing entry dropped)",
                );
                ColdRehydration::Absent
            }
            Err(err) => {
                self.offheap_read_failures.fetch_add(1, Ordering::Relaxed);
                tracing::error!(
                    instance_key = key,
                    error = %err,
                    "cold rehydration: cold instance's row could not be read; keeping it \
                     cold and routable (will retry)"
                );
                ColdRehydration::Unreadable
            }
        }
    }

    /// Restores spilled root variables of the instance an instance-addressed
    /// command is about to evaluate expressions against. Migration and
    /// modification may arm new boundary/catch subscriptions whose correlation
    /// keys / timer expressions read root variables; job activation (the only
    /// other restore path) is not part of either command, so without this the
    /// engine would evaluate against the empty spill placeholder.
    ///
    /// Fails closed like the cold path (#1388): an unreadable variable row
    /// rejects the command with the retryable
    /// [`EngineError::InstanceUnavailable`] instead of running it against an
    /// instance missing its variables.
    fn ensure_variables_resident_for_command(
        &mut self,
        command: &Command,
    ) -> Result<(), EngineError> {
        let instance_key = match command {
            Command::MigrateInstance { instance_key, .. }
            | Command::ModifyInstance { instance_key, .. } => *instance_key,
            _ => return Ok(()),
        };
        if !self.engine.is_variables_spilled(instance_key) {
            return Ok(());
        }
        self.restore_spilled_variables(instance_key, "command")
            .map_err(|err| {
                tracing::error!(
                    instance_key,
                    error = %err,
                    "command: spilled instance's variable row could not be read; the \
                     command is rejected as retryable (row kept)"
                );
                EngineError::InstanceUnavailable { instance_key }
            })?;
        // Post-condition, not just the restore's verdict: the restore is a
        // no-op when no spill tier is wired, which would otherwise let the
        // command evaluate against the empty placeholder (Copilot on #1418).
        if self.engine.is_variables_spilled(instance_key) {
            tracing::error!(
                instance_key,
                "command: spilled instance's variables are still not resident (no \
                 spill tier wired?); the command is rejected as retryable"
            );
            return Err(EngineError::InstanceUnavailable { instance_key });
        }
        Ok(())
    }

    /// Rehydrates any cold instance a command is about to target, so the engine
    /// processes the command against complete hot state. Cheap when cold spill is
    /// unset or nothing relevant is cold (a few index lookups). Routing mirrors
    /// the engine's own: by job key (`CompleteJob`/`FailJob`/...), user-task key,
    /// incident key, message name+correlation (`CorrelateMessage`), timer due-time
    /// (`TriggerTimers`), the instance key (`CancelInstance`), or an instance-root
    /// or element-instance scope key (`SetVariables`).
    ///
    /// Fails closed (#1388): if any targeted cold instance's row is unreadable
    /// the command is rejected with the retryable
    /// [`EngineError::InstanceUnavailable`] BEFORE it reaches the engine, so it
    /// can never run against state missing that instance (a misleading 404, or
    /// a message correlated/buffered as if the cold subscriber did not exist).
    fn ensure_resident_for_command(&mut self, command: &Command) -> Result<(), EngineError> {
        self.ensure_variables_resident_for_command(command)?;
        let Some(cold) = self.cold.as_ref() else {
            return Ok(());
        };
        let mut targets: Vec<Key> = Vec::new();
        match command {
            Command::CompleteJob { job_key, .. }
            | Command::FailJob { job_key, .. }
            | Command::ThrowJobError { job_key, .. }
            | Command::UpdateJobRetries { job_key, .. }
            | Command::UpdateJobTimeout { job_key, .. }
            | Command::UpdateJob { job_key, .. } => {
                targets.extend(cold.index.instance_for_job(*job_key));
            }
            // Opportunistic batch commands (#1388): locally, an unreadable
            // candidate must not fail the whole poll/tick closed (that would
            // let one bad row starve every healthy job or timer). Rehydrate
            // best-effort — the unreadable instance stays cold and is retried
            // next time. Under replicated (Raft) apply the skip would let a
            // healthy and an impaired replica apply the same committed entry
            // to DIFFERENT job/timer sets, so it fails closed instead (#1470).
            Command::ActivateJobs {
                job_type, max_jobs, ..
            } => {
                let (job_type, max_jobs) = (job_type.clone(), *max_jobs);
                if self.strict_cold_reads() {
                    return self.rehydrate_activation_candidates_strict(&job_type, max_jobs);
                }
                self.rehydrate_activation_candidates(&job_type, max_jobs);
                return Ok(());
            }
            Command::ActivateJobsByKey { job_keys, .. } => {
                targets.extend(
                    job_keys
                        .iter()
                        .filter_map(|key| cold.index.instance_for_job(*key)),
                );
            }
            Command::AssignUserTask { user_task_key, .. }
            | Command::UnassignUserTask { user_task_key }
            | Command::UpdateUserTask { user_task_key, .. }
            | Command::CompleteUserTask { user_task_key, .. } => {
                targets.extend(cold.index.instance_for_user_task(*user_task_key));
            }
            Command::ResolveIncident { incident_key, .. } => {
                targets.extend(cold.index.instance_for_incident(*incident_key));
            }
            Command::SetVariables { scope_key, .. } => {
                // The scope may be the instance root or one of its active
                // element-instance (token) scopes; both can address a cold
                // instance and must page it back in before the update applies.
                if cold.index.contains(*scope_key) {
                    targets.push(*scope_key);
                } else {
                    targets.extend(cold.index.instance_for_scope(*scope_key));
                }
            }
            Command::CancelInstance { instance_key } => {
                if cold.index.contains(*instance_key) {
                    targets.push(*instance_key);
                }
            }
            Command::ModifyInstance { instance_key, .. }
            | Command::MigrateInstance { instance_key, .. } => {
                // Modify/migrate address a single instance (activating/terminating its
                // element instances); page it in before the command applies.
                if cold.index.contains(*instance_key) {
                    targets.push(*instance_key);
                }
            }
            Command::CorrelateMessage {
                message_name,
                correlation_key,
                ..
            } => {
                targets.extend(
                    cold.index
                        .instances_for_message(message_name, correlation_key),
                );
            }
            Command::TriggerTimers { now } => {
                let strict = self.strict_cold_reads();
                let mut unavailable = None;
                for key in cold.index.instances_due(*now) {
                    // Locally an unreadable row stays cold and armed; the next
                    // tick retries. Under Raft apply it fails the entry closed
                    // (#1470) so no replica fires a different timer set.
                    if self.rehydrate_cold(key) == ColdRehydration::Unreadable && strict {
                        unavailable.get_or_insert(key);
                    }
                }
                return match unavailable {
                    Some(instance_key) => Err(EngineError::InstanceUnavailable { instance_key }),
                    None => Ok(()),
                };
            }
            _ => {}
        }
        let mut unavailable = None;
        for key in targets {
            if self.rehydrate_cold(key) == ColdRehydration::Unreadable {
                unavailable.get_or_insert(key);
            }
        }
        // Instance-wide teardown/restructuring commands also reach the
        // instance's call-activity descendants (terminating a call activity
        // terminates its child): page every cold descendant in too, or the
        // engine's resident-only child lookup misses it and the child later
        // resumes orphaned work (Copilot on #1418).
        if let Command::ModifyInstance { instance_key, .. }
        | Command::MigrateInstance { instance_key, .. }
        | Command::CancelInstance { instance_key } = command
            && let Some(key) = self.rehydrate_cold_descendants(*instance_key)
        {
            unavailable.get_or_insert(key);
        }
        match unavailable {
            Some(instance_key) => Err(EngineError::InstanceUnavailable { instance_key }),
            None => Ok(()),
        }
    }

    /// Rehydrates every cold call-activity descendant of `root` (through
    /// resident and cold intermediates alike). Returns the first descendant
    /// whose row is unreadable, if any (it stays cold; the caller fails
    /// closed).
    fn rehydrate_cold_descendants(&mut self, root: Key) -> Option<Key> {
        // Resident parent → children, built ONCE: scanning `instances` per
        // visited descendant is O(N × D) on the partition command path (Copilot
        // on #1418). A rehydrated child is pushed onto the frontier itself, so
        // its own children are looked up under its key — the map needs no
        // re-keying as instances page back in.
        let mut resident_children: HashMap<Key, Vec<Key>> = HashMap::new();
        for instance in self.engine.state().instances.values() {
            if let Some(parent) = instance.parent_process_instance_key {
                resident_children
                    .entry(parent)
                    .or_default()
                    .push(instance.key);
            }
        }
        let mut unavailable = None;
        let mut frontier = vec![root];
        let mut seen: HashSet<Key> = HashSet::new();
        while let Some(parent) = frontier.pop() {
            if !seen.insert(parent) {
                continue;
            }
            let cold_children = self
                .cold
                .as_ref()
                .map(|cold| cold.index.children_of(parent))
                .unwrap_or_default();
            for child in cold_children {
                match self.rehydrate_cold(child) {
                    ColdRehydration::Unreadable => {
                        unavailable.get_or_insert(child);
                    }
                    _ => frontier.push(child),
                }
            }
            frontier.extend(
                resident_children
                    .get(&parent)
                    .into_iter()
                    .flatten()
                    .copied(),
            );
        }
        unavailable
    }

    /// Sheds dormant instances to disk when hot RAM crosses the high-water mark.
    /// Cheap below the mark (one resident-bytes read). Above it, snapshots the
    /// oldest cold-spillable instances in bounded batches — lifting each whole
    /// instance (control state, jobs, timers, subscriptions) out of the engine
    /// and into the store, recording only its routing facets in the resident
    /// index — until resident memory falls under the low-water mark or no dormant
    /// instance remains. Run from the background tick on the engine thread. The
    /// snapshots are derived from the durable journal, so a lost one is
    /// reconstructable: this is a memory cache, not a system of record.
    pub fn maybe_cold_spill(&mut self) {
        if self.fenced {
            return;
        }
        let (high, low, shedding) = match self.cold.as_ref() {
            Some(cold) if cold.high_water > 0 => (cold.high_water, cold.low_water, cold.shedding),
            _ => return,
        };
        let Some(resident) = crate::memory::resident_bytes() else {
            return;
        };
        let resident = resident as u64;
        // Hysteresis: engage at `high_water`, then keep shedding across ticks until
        // resident falls under `low_water` (the OOM guard's reclaim target).
        let threshold = if shedding { low } else { high };
        if resident < threshold {
            if shedding && let Some(cold) = self.cold.as_mut() {
                cold.shedding = false;
            }
            return;
        }
        // Total: the match above returned unless `self.cold` is `Some`, so the
        // spill state is present here by construction.
        // expect_used: invariant by construction — see the match guard above.
        #[allow(
            clippy::expect_used,
            reason = "self.cold is Some by the match guard at function entry"
        )]
        let cold = self.cold.as_mut().expect("cold set");
        cold.shedding = true;
        let store = Arc::clone(&cold.store);
        // Evict dormant instances under a bounded per-tick wall-clock budget
        // ([`SPILL_TICK_BUDGET`]), then purge ONCE — same rationale as the variable
        // sweep: holding the single writer through a `shrink()` + `purge()` per
        // 64-instance batch until resident fell under the low-water mark stalled
        // create/complete under a deep backlog. Successive ticks resume where this
        // one left off (latched by `shedding`).
        let deadline = std::time::Instant::now() + SPILL_TICK_BUDGET;
        let mut spilled = 0usize;
        loop {
            let batch = self.engine.cold_spillable_instances(64);
            if batch.is_empty() {
                break;
            }
            let mut spilled_in_batch = 0usize;
            for key in batch {
                let Some(snapshot) = self.engine.snapshot_instance(key) else {
                    continue;
                };
                if store.put_cold(key, &snapshot).is_ok() {
                    // Total: `self.cold` is `Some` for the whole sweep (bound
                    // above and never cleared here).
                    if let Some(cold) = self.cold.as_mut() {
                        cold.index.insert(&snapshot);
                    }
                    spilled_in_batch += 1;
                } else {
                    // Store write failed: keep the instance resident rather than
                    // drop it from hot state with no durable cold copy.
                    self.engine.rehydrate_instance(snapshot);
                }
            }
            spilled += spilled_in_batch;
            if spilled_in_batch == 0 || std::time::Instant::now() >= deadline {
                break;
            }
        }
        if spilled > 0 {
            let _ = crate::memory::purge();
            let cold_len = self.cold.as_ref().map_or(0, |cold| cold.index.len());
            tracing::info!(
                "cold spill: shed {spilled} dormant instance(s) to disk ({cold_len} now cold, budgeted)",
            );
            // Clear the latch once this sweep has dropped resident under the
            // low-water target. Cold spill removes *whole* instances (unlike a
            // variable spill), so the engine's state maps now carry dead capacity:
            // compact them ONCE here, at the end of the shedding episode, rather
            // than `shrink_to_fit`-ing all ten maps on every firing tick.
            if crate::memory::resident_bytes().is_some_and(|now| (now as u64) < low) {
                if let Some(cold) = self.cold.as_mut() {
                    cold.shedding = false;
                }
                self.engine.shrink();
            }
        }
    }

    /// Test hook: whether cold-spill is configured (a cold store is wired). Used
    /// to assert that lazily-created Raft replica engines inherit the same spill
    /// tiers as owned engines, so a follower can reclaim hot RAM.
    // Also compiled under `test-hooks` (enabled by the gateway bin's
    // dev-dependencies) so the bin's own inline tests — which link this crate as
    // a non-test library since the ADR 0064 extraction — keep seeing the hook.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn cold_spill_configured(&self) -> bool {
        self.cold.is_some()
    }

    /// Test hook: cold-spills every currently dormant instance regardless of the
    /// memory watermark, returning the number shed. Lets tests exercise the
    /// spill/rehydrate seams deterministically without driving real RAM pressure.
    /// Also compiled under `test-hooks` so downstream crates' tests (the Raft
    /// snapshot-streaming test, #1337) can force the cold tier deterministically.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn force_cold_spill_all(&mut self) -> usize {
        let store = match self.cold.as_ref() {
            Some(cold) => Arc::clone(&cold.store),
            None => return 0,
        };
        let mut spilled = 0usize;
        loop {
            let batch = self.engine.cold_spillable_instances(64);
            if batch.is_empty() {
                break;
            }
            let mut spilled_in_batch = 0usize;
            for key in batch {
                let Some(snapshot) = self.engine.snapshot_instance(key) else {
                    continue;
                };
                if store.put_cold(key, &snapshot).is_ok() {
                    // Total: `self.cold` is `Some` for the whole sweep (bound
                    // above and never cleared here).
                    if let Some(cold) = self.cold.as_mut() {
                        cold.index.insert(&snapshot);
                    }
                    spilled_in_batch += 1;
                } else {
                    self.engine.rehydrate_instance(snapshot);
                }
            }
            spilled += spilled_in_batch;
            if spilled_in_batch == 0 {
                break;
            }
        }
        spilled
    }

    /// Test hook: rehydrates every cold instance back into hot state through the
    /// production rehydration path, returning `(restored, failed)` where `failed`
    /// counts instances that were absent or unreadable. Lets a replica-parity
    /// test (#100) compare full per-instance/per-job state across replicas after
    /// proving each replica's off-heap rows are readable.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn rehydrate_all_cold(&mut self) -> (usize, usize) {
        let keys: Vec<Key> = match self.cold.as_ref() {
            Some(cold) => cold.index.keys().collect(),
            None => return (0, 0),
        };
        let mut restored = 0usize;
        let mut failed = 0usize;
        for key in keys {
            match self.rehydrate_cold(key) {
                ColdRehydration::Restored => restored += 1,
                ColdRehydration::NotCold => {}
                ColdRehydration::Absent | ColdRehydration::Unreadable => failed += 1,
            }
        }
        (restored, failed)
    }

    /// Wires the read-model exporter channel. Set before any command is applied
    /// (including demo seeding) so every journaled event is projected.
    ///
    /// For a journal backed by a **segmented shared writer** the exporter is
    /// dropped into the writer's cell instead of `self.exporter`, so the writer
    /// forwards events to the read model in fsync (log) order — keeping
    /// `exported_position` a true global prefix across partitions — and
    /// `persist()` does not also forward (which would double-project).
    ///
    /// `queued` is the partition's exporter-queue byte gauge, wired into the
    /// shared cell so the writer can account each forwarded command's size for
    /// create-admission backpressure. It is unused on the non-shared path (which
    /// does not participate in exporter-queue accounting; `bytes` is always 0
    /// there).
    pub fn set_exporter(&mut self, exporter: Sender<ExportBatch>, queued: Arc<AtomicU64>) {
        if let Some(cell) = self.shared_exporter.as_ref() {
            cell.lock()
                .unwrap_or_else(|e| e.into_inner())
                .insert(self.partition_id, (exporter, queued));
        } else {
            self.exporter = Some(exporter);
        }
    }

    /// Evicts a batch of completed instances in a single pass over hot state.
    /// Mirrors [`Engine::evict_instances`]. Used on the steady-state exporter
    /// path once the read model has the completions. Also drops any spilled
    /// variable / cold rows the evicted (now terminal) instances left behind, so
    /// the spill store stays bounded to the live backlog rather than accumulating
    /// orphan payloads for every completed or cancelled instance.
    pub fn evict_instances(&mut self, keys: &[Key]) -> usize {
        if let Some(store) = self.spill_store() {
            // A spill-delete failure is latched into the store's observable
            // `forget_faults` (#1366); the eviction itself is hot-state only and
            // must still proceed, so the typed error is surfaced there rather
            // than aborting the eviction pass.
            let _ = store.forget(keys);
        }
        self.engine.evict_instances(keys)
    }

    /// Unconditionally retires a batch of instances from hot state (and their
    /// spilled/cold rows) regardless of local lifecycle state. The follower-side
    /// counterpart to [`Journal::evict_instances`]: a partition leader broadcasts
    /// the authoritative retirement digest (the keys it completed + exporter-evicted
    /// locally, which never enter the raft log) and each follower replica drops them
    /// here, so its replica engine cannot pile up completed instances as never-reaped
    /// `Active` shells (the RF>1 leader-local-completion hot-state leak). Mirrors
    /// [`Engine::retire_instances`]; keys absent locally are ignored.
    pub fn retire_instances(&mut self, keys: &[Key]) -> usize {
        if let Some(store) = self.spill_store() {
            store.forget_async(keys);
        }
        // Drop any cold-index entries for these keys too: the digest path forgets
        // the disk rows above, but a cold-spilled replica instance named in the
        // digest would otherwise leave its in-RAM ColdIndex entry behind (a slow
        // leak, and a stale routing entry for a row that no longer exists).
        if let Some(cold) = self.cold.as_mut() {
            for &key in keys {
                cold.index.remove(key);
            }
        }
        self.engine.retire_instances(keys)
    }

    /// Owned-`Vec` variant of [`Journal::retire_instances`] for the digest apply
    /// path, whose caller ([`apply_retirement_digest`](crate::AppState::apply_retirement_digest))
    /// already owns the key vec. Moves the vec into the forget worker instead of
    /// cloning it (this runs on the single-writer replica actor and the set can be
    /// large), mirroring the [`Journal::retire_below`] copy-free change.
    pub fn retire_instances_owned(&mut self, keys: Vec<Key>) -> usize {
        // Consume the borrowed work first, then move `keys` into the worker queue.
        if let Some(cold) = self.cold.as_mut() {
            for &key in &keys {
                cold.index.remove(key);
            }
        }
        let n = self.engine.retire_instances(&keys);
        if let Some(store) = self.spill_store() {
            store.forget_async_owned(keys);
        }
        n
    }

    /// The shared disk-backed spill/cold store, if either tier is wired. Both
    /// tiers share one [`VarSpillStore`] (one file, one WAL), so either handle
    /// reaches the same `spill` and `cold` tables.
    fn spill_store(&self) -> Option<Arc<VarSpillStore>> {
        if let Some(vs) = self.spill.as_ref() {
            return Some(Arc::clone(&vs.store));
        }
        self.cold.as_ref().map(|c| Arc::clone(&c.store))
    }

    /// Evicts every completed instance from hot state (used after a boot replay,
    /// once the read model is caught up). Mirrors [`Engine::evict_completed`].
    pub fn evict_completed(&mut self) -> usize {
        self.engine.evict_completed()
    }

    /// The owner-side low-water mark broadcast to this partition's follower
    /// replicas. Passes through to [`Engine::retirement_low_water`].
    pub fn retirement_low_water(&self) -> Key {
        self.engine.retirement_low_water()
    }

    /// Follower-side reconciliation sweep: drops every resident replica instance
    /// below the owner's `low_water` (and forgets their spilled rows), bounded to
    /// `max_remove` per call. Mirrors [`Engine::retire_below`]; the loss-tolerant
    /// convergence backstop for the best-effort per-key retirement digest.
    ///
    /// Sweeps **both tiers**: [`Engine::retire_below`] reaps hot replica shells,
    /// and [`ColdIndex::retire_below`] reaps cold-spilled replica instances the
    /// engine can no longer see. Without the cold sweep, a cold instance whose
    /// per-key digest frame was dropped is never reaped by either path and leaks
    /// its `cold` row forever (the observed unbounded var-spill file growth).
    pub fn retire_below(&mut self, low_water: Key, max_remove: usize) -> usize {
        let mut reaped = self.engine.retire_below(low_water, max_remove);
        if let Some(cold) = self.cold.as_mut() {
            reaped.extend(cold.index.retire_below(low_water, max_remove));
        }
        if !reaped.is_empty()
            && let Some(store) = self.spill_store()
        {
            // Move the reaped set into the worker queue rather than cloning it —
            // this runs on the single-writer replica actor and the set can be large.
            let n = reaped.len();
            store.forget_async_owned(reaped);
            return n;
        }
        reaped.len()
    }

    /// Shrinks the hot-state map capacities to fit their live contents, returning
    /// the bucket arrays freed by prior eviction back to the allocator. Mirrors
    /// [`Engine::shrink`]; called on the idle-purge path, where steady-state
    /// eviction has already removed the entries but left the maps at peak
    /// capacity.
    pub fn shrink(&mut self) {
        self.engine.shrink();
    }

    /// Whether the journal started empty (no prior log).
    pub fn is_fresh(&self) -> bool {
        self.fresh
    }

    /// A **durability barrier** for a command that produced no events (#1181):
    /// the returned [`Commit`] resolves only after every write already enqueued
    /// on this journal's writer is durable (written and fsynced). An idempotent
    /// settlement replay applies with an empty event batch — nothing new to
    /// persist — but it must not be acknowledged before the ORIGINAL
    /// settlement it restates is durable: a crash in between would lose a
    /// settlement the worker was told succeeded, and the job would re-execute.
    /// The barrier rides the writer's strictly ordered queue (an empty write
    /// request), so it joins the same group-commit batch as the still-pending
    /// original and resolves with it. An in-memory journal (no writer) is
    /// already durable, so the barrier is ready immediately.
    pub fn barrier(&self) -> Commit {
        let Some(writer) = self.writer.as_ref() else {
            return Commit::ready();
        };
        let (ack, rx) = oneshot::channel();
        match writer.send(WriterMsg::Write(WriteRequest {
            bytes: Vec::new(),
            events: 0,
            partition: self.partition_id,
            events_arc: None,
            ack,
        })) {
            Ok(()) => {
                // The writer decrements `inflight` by one per processed
                // request, barriers included — account for it on enqueue.
                crate::metrics::inflight_inc();
                Commit(CommitInner::Pending(rx))
            }
            // The writer thread is gone (shutting down); treat as already
            // settled so callers never hang.
            Err(_) => Commit::ready(),
        }
    }

    /// Serializes `events` for the durable writer and forwards them to the
    /// read-model exporter, returning a [`Commit`] that resolves once they are
    /// fsynced. The events are shared via `Arc`, so the exporter handoff is a
    /// refcount bump rather than a deep copy of the (up to 50 KB) payloads — the
    /// single command thread never clones them. Empty batches and in-memory
    /// journals are already durable, so they return a ready commit.
    fn persist(&self, events: &Arc<Vec<Event>>) -> Commit {
        if events.is_empty() {
            return Commit::ready();
        }

        // Forward to the read-model exporter in command order (the actor applies
        // commands serially). Independent of disk persistence, so the in-memory
        // journal still feeds an in-memory read store. `Arc::clone` is cheap.
        //
        // A segmented shared-writer-backed journal does NOT forward here: its
        // writer forwards in fsync (log) order via the exporter cell (see
        // `set_exporter`), so `exported_position` stays a true global prefix
        // across partitions. Forwarding here too would double-project.
        let shared_backed = self.shared_exporter.is_some();
        if !shared_backed && let Some(exporter) = self.exporter.as_ref() {
            // Non-shared path (in-memory / legacy single-file): forward with a
            // zero byte estimate — this path has no exporter-queue backpressure
            // gauge, so it never accounts (and never sheds) here.
            let _ = exporter.send(ExportBatch {
                events: Arc::clone(events),
                bytes: 0,
            });
        }

        if self.writer.is_none() {
            return Commit::ready();
        }

        // The shared multi-partition segmented writer records each event line
        // with its GLOBAL write-partition tag (`<partition>\t<json>`), so
        // recovery can demultiplex by the partition that actually produced the
        // write rather than by the event's key. This matters for a clustered
        // node's durable replicated `ProcessDeployed`: it is journaled under the
        // node's first-owned partition but keyed to the deployment partition (0),
        // which a node not owning partition 0 could not otherwise route back. The
        // single-partition / legacy / in-memory paths keep the bare `<json>` line
        // format (their reader does not expect a tag).
        let bytes = self.serialize_events(events);
        self.send_write(bytes, events, shared_backed)
    }

    /// Serializes `events` to the journal's newline-terminated record format
    /// (`<partition>\t<json>` on the shared multi-partition writer, bare
    /// `<json>` otherwise — see [`persist`](Self::persist)).
    fn serialize_events(&self, events: &[Event]) -> Vec<u8> {
        let tagged = self.shared_exporter.is_some();
        let mut bytes = Vec::new();
        for event in events.iter() {
            if tagged {
                bytes.extend_from_slice(self.partition_id.to_string().as_bytes());
                bytes.push(b'\t');
            }
            // expect_used: `Event`'s serde derive is total (every field is a
            // serializable value type), so serialization cannot fail; `persist`
            // returns a `Commit` (no error channel), so there is no typed path
            // to propagate through here.
            #[allow(
                clippy::expect_used,
                reason = "Event serialization is total (serde derive over value types); Commit carries no error channel"
            )]
            let line = serde_json::to_string(event).expect("event serializes");
            bytes.extend_from_slice(line.as_bytes());
            bytes.push(b'\n');
        }
        bytes
    }

    /// Hands already-serialized `bytes` (carrying `events`) to the writer and
    /// returns their durability barrier.
    fn send_write(&self, bytes: Vec<u8>, events: &Arc<Vec<Event>>, shared_backed: bool) -> Commit {
        let Some(writer) = self.writer.as_ref() else {
            return Commit::ready();
        };
        let (ack, rx) = oneshot::channel();
        let events_count = events.len();
        let bytes_len = bytes.len();
        // Only the segmented shared-writer path needs the events Arc (for
        // log-order exporter forwarding) and partition tagging; everything else
        // leaves `events_arc` `None` to avoid an extra refcount/clone.
        let events_arc = shared_backed.then(|| Arc::clone(events));
        match writer.send(WriterMsg::Write(WriteRequest {
            bytes,
            events: events_count,
            partition: self.partition_id,
            events_arc,
            ack,
        })) {
            Ok(()) => {
                crate::metrics::inflight_inc();
                crate::metrics::journal_inflight_add(bytes_len);
                Commit(CommitInner::Pending(rx))
            }
            // The writer thread is gone (shutting down); treat as already settled
            // so callers never hang.
            Err(_) => Commit::ready(),
        }
    }

    /// Applies a durable command at logical instant `now`, journaling its events
    /// on success. Returns the events (shared via `Arc` with the read-model
    /// exporter) and a [`Commit`] the caller should await before acknowledging the
    /// request. Mirrors [`Engine::apply_command_at`].
    ///
    /// Under a Raft entry ([`begin_raft_entry`](Self::begin_raft_entry)) the
    /// events are BUFFERED, not written, and the returned [`Commit`] is
    /// already-ready: the entry's single durable barrier is the one
    /// [`commit_raft_entry`](Self::commit_raft_entry) returns (#1405).
    pub fn apply_command_at(
        &mut self,
        command: Command,
        now: u64,
    ) -> Result<(Arc<Vec<Event>>, Commit), EngineError> {
        if self.fenced {
            // Environmental + retryable: a peer (or this replica once rebuilt
            // from its journal) applies the command; this one must not (#1405).
            return Err(EngineError::InstanceUnavailable { instance_key: 0 });
        }
        self.ensure_resident_for_command(&command)?;
        let settles_job = matches!(
            command,
            Command::CompleteJob { .. } | Command::FailJob { .. } | Command::ThrowJobError { .. }
        );
        let events = Arc::new(self.engine.apply_command_at(command, now)?);
        let mut activated: Vec<_> = events
            .iter()
            .filter_map(|event| match event {
                Event::JobActivated { job_key, .. } => self.engine.activated_job(*job_key),
                _ => None,
            })
            .collect();
        self.rehydrate_activated_variables(&mut activated);
        if let Some(buffer) = self.raft_entry.as_mut() {
            // Raft entry (#1405): buffer for the entry's atomic frame. No
            // spill either — spilling state whose events are not yet durable
            // would let the off-heap store run ahead of the journal; the
            // deferred `maybe_spill` runs in `commit_raft_entry`.
            buffer.extend(events.iter().cloned());
            return Ok((events, Commit::ready()));
        }
        // An eventless success (an idempotent settlement replay, #1181) has
        // nothing new to persist, but acknowledging it must still wait until
        // the ORIGINAL settlement it restates is durable — otherwise a crash
        // loses an acknowledged settlement and the job re-executes. `persist`
        // short-circuits empty batches to a ready commit, so take a writer
        // barrier instead: it resolves once every write enqueued before it
        // (the original included) is fsynced.
        // Only settlement commands can be idempotent replays; any other eventless
        // no-op (e.g. an idle expiry sweep) keeps its ready commit so it costs
        // no fsync and no commit statistics.
        let commit = if events.is_empty() && settles_job {
            self.barrier()
        } else {
            self.persist(&events)
        };
        self.maybe_spill();
        Ok((events, commit))
    }

    /// Arms entry-level atomicity for one committed Raft entry (#1405): until
    /// [`commit_raft_entry`](Self::commit_raft_entry) or
    /// [`abort_raft_entry`](Self::abort_raft_entry), applied commands buffer
    /// their events instead of journaling them.
    pub fn begin_raft_entry(&mut self) {
        self.raft_entry = Some(Vec::new());
    }

    /// Journals the armed Raft entry as ONE atomic frame (#1405): every
    /// buffered event followed by an [`Event::RaftEntryApplied`] record for
    /// `index` (`raft_state` is the replication layer's opaque log-id +
    /// membership), with a single durability barrier. A torn frame is dropped
    /// whole on recovery, so a crash can never keep part of an entry. Returns
    /// the entry's events (the record included, for log-order export) and the
    /// barrier. Errors — journaling nothing — when the frame would exceed the
    /// frame cap; the caller must fail closed.
    pub fn commit_raft_entry(
        &mut self,
        index: u64,
        raft_state: String,
    ) -> io::Result<(Arc<Vec<Event>>, Commit)> {
        if self.fenced {
            self.raft_entry = None;
            return Err(io::Error::other(
                "journal fenced after an aborted Raft entry; rebuild from disk (#1405)",
            ));
        }
        let mut events = self.raft_entry.take().unwrap_or_default();
        events.push(Event::RaftEntryApplied {
            index,
            raft_state: raft_state.clone(),
            start_dispatch_rr: self.engine.start_dispatch_rr(),
        });
        let events = Arc::new(events);
        let shared_backed = self.shared_exporter.is_some();
        let commit = if self.writer.is_some() {
            let compress = self.seg.as_ref().is_some_and(|s| s.compress);
            let frame = crate::seglog::frame_atomic(
                &self.serialize_events(&events),
                events.len() as u64,
                compress,
            )?;
            if !shared_backed && let Some(exporter) = self.exporter.as_ref() {
                let _ = exporter.send(ExportBatch {
                    events: Arc::clone(&events),
                    bytes: 0,
                });
            }
            self.send_write(frame, &events, shared_backed)
        } else {
            // In-memory journal: nothing to frame, but the exporter still sees
            // the entry exactly as `persist` would forward it.
            self.persist(&events)
        };
        self.engine.set_raft_applied(index, raft_state);
        self.maybe_spill();
        Ok((events, commit))
    }

    /// Disarms the Raft entry WITHOUT journaling anything (#1405): the entry
    /// failed closed on an environmental error. The live engine state may hold
    /// the entry's partial, non-durable effects; the replica must shut down and
    /// rebuild from the durable journal, which never saw them.
    pub fn abort_raft_entry(&mut self) {
        self.raft_entry = None;
        self.fenced = true;
    }

    /// Whether this journal is fenced after an aborted Raft entry (#1405, see
    /// the `fenced` field).
    pub fn is_fenced(&self) -> bool {
        self.fenced
    }

    /// The last Raft applied-entry record this journal wrote or recovered
    /// (#1405): `(raft log index, opaque replication state)`.
    pub fn raft_applied(&self) -> Option<(u64, String)> {
        self.engine.raft_applied().cloned()
    }

    /// The authoritative ownership tuple `(element_id, process_instance_key)` of
    /// the agent instance identified by `agent_instance_key`, resolved on the
    /// engine thread against primary state. The REST `PATCH` handler uses it to
    /// fill the ownership assertion [`Command::UpdateAgentInstance`] requires but
    /// the 8.10 request body omits, without racing the read-model projection.
    pub fn agent_instance_ownership(
        &self,
        agent_instance_key: Key,
    ) -> Option<(nanobpmn_engine_core::ElementId, Key)> {
        self.engine.agent_instance_ownership(agent_instance_key)
    }

    /// Applies a durable command using the engine's current clock, journaling its
    /// events on success. Mirrors [`Engine::apply_command`].
    pub fn apply_command(
        &mut self,
        command: Command,
    ) -> Result<(Arc<Vec<Event>>, Commit), EngineError> {
        if self.fenced {
            return Err(EngineError::InstanceUnavailable { instance_key: 0 });
        }
        self.ensure_resident_for_command(&command)?;
        let events = Arc::new(self.engine.apply_command(command)?);
        let commit = self.persist(&events);
        self.maybe_spill();
        Ok((events, commit))
    }

    /// Per-command variable-spill check. Sheds the oldest backlog's variables to
    /// the spill store when the resident spill-candidate set exceeds the mode's
    /// per-command cap (the fixed budget in `Budget` mode, or the emergency
    /// `hard_cap` in `Adaptive` mode). The variables are already durable in the
    /// journal, so a spill write that is later lost is reconstructable — it is a
    /// cache, not a system of record, which makes the exact shed timing a soft
    /// bound we can amortise.
    ///
    /// Hot-path cost: an O(1) resident-count guard skips everything while the
    /// whole resident set fits the cap (the steady state — and in `Adaptive` mode
    /// the normal case, since its real trigger is the RSS sweep). Once
    /// persistently over the cap, the precise O(N) spillable scan/shed runs only
    /// once every [`SPILL_CHECK_INTERVAL`] commands rather than on every apply, so
    /// a deep backlog no longer pays an O(N) scan per command (which was quadratic
    /// in the backlog and drove congestion collapse).
    /// Approximate resident variable-payload bytes held by this partition's
    /// instances (spilled instances contribute ~0). Attribution gauge for the
    /// burst RSS balloon — see [`Engine::resident_variable_bytes`]. O(N); called
    /// off the hot path by the mem-pressure sampler.
    pub fn resident_variable_bytes(&self) -> u64 {
        self.engine.resident_variable_bytes()
    }

    fn maybe_spill(&mut self) {
        if self.fenced {
            return;
        }
        let Some(cap) = self
            .spill
            .as_ref()
            .and_then(|vs| vs.trigger.per_command_cap())
        else {
            return;
        };
        // O(1) fast path: spill candidates are a subset of resident instances, so
        // when the resident set already fits the cap nothing can be over it.
        if self.engine.resident_instance_count() <= cap {
            self.spill_check_skip = 0;
            return;
        }
        // Over the cheap bound: amortise the precise scan/shed across commands.
        if self.spill_check_skip > 0 {
            self.spill_check_skip -= 1;
            return;
        }
        self.spill_check_skip = SPILL_CHECK_INTERVAL;
        self.shed_variables(cap);
    }

    /// Sheds the oldest spill-candidate instances' variables to disk until the
    /// resident spill-candidate count is back down to `target`. Returns how many
    /// instances were shed. Shared by the per-command cap check and the adaptive
    /// RAM-pressure sweep so the two triggers never diverge on *how* they shed.
    fn shed_variables(&mut self, target: usize) -> usize {
        let resident = self.engine.resident_spillable_count();
        if resident <= target {
            return 0;
        }
        // Lean mode: spill write-through targets the authoritative durable store
        // (the row must be durable before the payload leaves RAM, since the store
        // is the only copy recovery reads). Classic mode uses the destructive
        // spill cache.
        let varstore = self.varstore.clone();
        let spill_store = self.spill.as_ref().map(|vs| Arc::clone(&vs.store));
        if varstore.is_none() && spill_store.is_none() {
            return 0;
        }
        let over = resident - target;
        let mut shed = 0;
        for key in self.engine.spillable_instances(over) {
            if let Some(vars) = self.engine.spill_variables(key) {
                let ok = if let Some(vstore) = varstore.as_ref() {
                    vstore.put_current(key, &vars).is_ok()
                } else if let Some(spill) = spill_store.as_ref() {
                    // Total: the early return above guarantees at least one of
                    // varstore / spill_store is `Some`; with varstore `None`
                    // here, the spill store is present by construction.
                    spill.put(key, &vars).is_ok()
                } else {
                    false
                };
                if ok {
                    shed += 1;
                } else {
                    // Spill failed: keep the payload resident rather than lose it.
                    self.engine.rehydrate_variables(key, vars);
                }
            }
        }
        shed
    }

    /// Adaptive variable spill — the RAM-pressure tier. Run on the periodic sweep
    /// (alongside [`maybe_cold_spill`](Journal::maybe_cold_spill), and *before* it:
    /// shedding an instance's variables keeps it live and is cheaper than evicting
    /// the whole instance). Only active in [`Adaptive`](VarSpillTrigger::Adaptive)
    /// mode; a no-op otherwise (`Budget`/off self-regulate per command).
    ///
    /// **Memory is the sole trigger.** Spill exists to bound resident memory, so
    /// it fires strictly on measured memory — jemalloc `resident` over the
    /// `high_water` mark, or live system-available memory below `reserve` — and
    /// sheds toward the reclaim target. Instance *count* is deliberately not a
    /// signal: a count is a poor proxy for bytes (N tiny-var instances is trivial
    /// RAM, N large-var instances is not), and steering on it throttles throughput
    /// for a vanity metric. A large but *stable* working set below the mark keeps
    /// every variable resident at full throughput.
    ///
    /// Once over the mark it sheds the oldest active-backlog variables in batches
    /// — compacting and purging between batches so the reading reflects the shed —
    /// until resident memory falls under the reclaim target or nothing spillable
    /// remains. There is no RSS-delta bail: under concurrent inbound allocation a
    /// shed batch may not visibly drop RSS even though it helped, so the loop
    /// steers to the memory target and stops only when genuinely under it (or out
    /// of candidates). A single up-front byte check
    /// ([`VAR_SPILL_MIN_RELIEF_DIVISOR`]) skips the sweep when variables are too
    /// small a fraction of the overshoot to matter, so a tiny-payload backlog is
    /// not thrashed for negligible relief.
    pub fn maybe_var_spill_pressure(&mut self) {
        if self.fenced {
            return;
        }
        let (floor, high, low, reserve) = match self.spill.as_ref().map(|vs| &vs.trigger) {
            Some(VarSpillTrigger::Adaptive {
                floor,
                high_water,
                low_water,
                reserve,
                ..
            }) if *high_water > 0 => (*floor, *high_water, *low_water, *reserve),
            _ => return,
        };
        let Some(resident) = crate::memory::resident_bytes() else {
            return;
        };
        let resident = resident as u64;

        // Pure memory trigger: resident heap over the high-water OOM mark, or live
        // system-available memory below the reserve (another tenant eating RAM).
        // Nothing else — no instance-count trend.
        let avail_pressure =
            reserve > 0 && crate::memory::available_bytes().is_some_and(|a| a < reserve);
        if resident < high && !avail_pressure {
            // Below the mark: use free RAM at full throughput.
            return;
        }
        // Reclaim to the floor budget on external (reserve) pressure — free RAM is
        // contended, get aggressive; otherwise relax only to the low-water mark
        // (hysteresis below high-water).
        let target_low = if avail_pressure { floor } else { low };

        // Futile-shed guard (byte-based, not count-based): if the resident variable
        // payloads we could shed are a small fraction of the overshoot, variables
        // are not the memory driver — shedding them thrashes the backlog for ~no
        // relief. Hand the residual to cold spill / admission backpressure. Decided
        // once, up front, from a direct byte measurement rather than a noisy
        // per-batch RSS delta.
        let overshoot = resident.saturating_sub(target_low);
        let spillable_bytes = self.engine.resident_variable_bytes();
        if spillable_bytes < overshoot / VAR_SPILL_MIN_RELIEF_DIVISOR {
            return;
        }

        // Shed the oldest active-backlog variables under a bounded per-tick
        // wall-clock budget ([`SPILL_TICK_BUDGET`]), then purge ONCE. The original
        // design ran `shrink()` + jemalloc `purge()` + a resident re-read after
        // *every* batch, looping until resident fell under the target — a >100 ms
        // single-writer hold per tick under a deep backlog that starved
        // create/complete. Now the hold is bounded: shed what fits the budget,
        // then return the freed pages to the OS once so the next sweep's resident
        // reading is accurate (jemalloc's background thread would otherwise defer
        // the page return past its decay window and over-fire the RSS gate).
        //
        // No `shrink()` here: a variable spill keeps the *instance* resident (only
        // its `variables` map is emptied), so the engine's state maps do not
        // shrink — a `shrink_to_fit` across all ten of them would reallocate huge
        // maps for zero relief. The relief is the freed variable payloads, which
        // `purge()` returns to the OS. Successive sweeps converge under the mark.
        let deadline = std::time::Instant::now() + SPILL_TICK_BUDGET;
        let mut total = 0usize;
        loop {
            let resident_candidates = self.engine.resident_spillable_count();
            if resident_candidates == 0 {
                break;
            }
            let target = resident_candidates.saturating_sub(VAR_SPILL_SWEEP_BATCH);
            let shed = self.shed_variables(target);
            total += shed;
            if shed == 0 || std::time::Instant::now() >= deadline {
                break;
            }
        }
        if total > 0 {
            let _ = crate::memory::purge();
            tracing::info!(
                "variable spill (adaptive): shed {total} instance(s)' variables \
                 (resident {} MiB -> target {} MiB, budgeted)",
                resident / (1024 * 1024),
                target_low / (1024 * 1024),
            );
        }
    }

    /// Test hook: force an immediate, un-throttled spill scan. Production spills
    /// are amortised (see [`maybe_spill`]/[`SPILL_CHECK_INTERVAL`]), so tests that
    /// assert on the fully-shed state drive it deterministically through here
    /// instead of depending on the amortisation cadence.
    // Also compiled under `test-hooks` (enabled by the gateway bin's
    // dev-dependencies) so the bin's inline tests — the Raft replica-parity
    // guards (#100), which run under `VarSpillCfg::Budget(0)` where the
    // RAM-pressure sweep is a no-op — can force the per-command budget shed
    // deterministically instead of waiting out the 256-command amortisation.
    #[cfg(any(test, feature = "test-hooks"))]
    pub fn force_spill_scan(&mut self) {
        self.spill_check_skip = 0;
        self.maybe_spill();
    }

    /// Activates jobs **without** journaling: activation locks are volatile lease
    /// state, forfeited on restart. Mirrors [`Engine::activate_jobs`].
    ///
    /// When variable spill is wired, an activated job's instance may have had its
    /// variables shed to disk. Activation is exactly the moment they are needed
    /// again (the worker receives them, and completion may follow), so each
    /// spilled instance is rehydrated here: its payload is taken back from the
    /// store, restored into hot state, and used to fill the activated job. This
    /// is the read side of the memory/disk fusion — a single keyed SQLite read,
    /// served from the page cache for a warm working set.
    pub fn activate_jobs(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
    ) -> Vec<ActivatedJob> {
        self.activate_jobs_with_fetch(job_type, worker, max_jobs, timeout, now, Vec::new())
    }

    /// Like [`Journal::activate_jobs`] but records the declared read-set
    /// (`fetchVariables`) on each resulting [`Event::JobActivated`], so the
    /// activation's read provenance is journaled durably. An empty
    /// `fetch_variables` is exactly [`Journal::activate_jobs`] and keeps the
    /// activation event byte-identical.
    pub fn activate_jobs_with_fetch(
        &mut self,
        job_type: impl Into<String>,
        worker: impl Into<String>,
        max_jobs: usize,
        timeout: u64,
        now: u64,
        fetch_variables: Vec<String>,
    ) -> Vec<ActivatedJob> {
        if self.fenced {
            // Fenced after an aborted entry (#1405): the resident state holds a
            // non-durable prefix; never hand its jobs to a worker.
            return Vec::new();
        }
        let job_type = job_type.into();
        self.rehydrate_activation_candidates(&job_type, max_jobs);
        let mut activated = self.engine.activate_jobs_with_fetch(
            &job_type,
            worker,
            max_jobs,
            timeout,
            now,
            fetch_variables,
        );
        self.rehydrate_activated_variables(&mut activated);
        activated
    }

    pub fn select_activatable_job_keys(
        &mut self,
        job_type: &str,
        max_jobs: usize,
        now: u64,
        with_lease: bool,
    ) -> Vec<Key> {
        if self.fenced {
            return Vec::new(); // see `activate_jobs_with_fetch` (#1405)
        }
        self.rehydrate_activation_candidates(job_type, max_jobs);
        self.engine
            .select_activatable_job_keys(job_type, max_jobs, now, with_lease)
    }

    /// `true` while a command is applied as replicated (Raft) state-machine
    /// application — an armed Raft entry or a replicated-apply journal (#1470).
    /// There a cold-read failure must fail the command closed: skipping it
    /// best-effort would make replicas with differing off-heap readability
    /// apply the same committed entry to different results.
    fn strict_cold_reads(&self) -> bool {
        self.replicated_apply || self.raft_entry.is_some()
    }

    /// Replicated-apply variant of
    /// [`rehydrate_activation_candidates`](Self::rehydrate_activation_candidates)
    /// (#1470): pages in EVERY cold instance holding a job of this type (not
    /// just `max_jobs` of them), in
    /// deterministic key order (no replica-local rotation cursor), and fails
    /// closed with [`EngineError::InstanceUnavailable`] on the first unreadable
    /// row, so an impaired replica never activates a different job set than a
    /// healthy one.
    fn rehydrate_activation_candidates_strict(
        &mut self,
        job_type: &str,
        max_jobs: usize,
    ) -> Result<(), EngineError> {
        let Some(cold) = self.cold.as_ref() else {
            return Ok(());
        };
        if max_jobs == 0 {
            return Ok(()); // a zero-job request activates nothing on any replica
        }
        // No `max_jobs` cutoff: which jobs win selection depends on engine
        // ordering across ALL candidates, and residency differs per replica.
        // Stopping early could leave a later cold-and-unreadable candidate
        // unread on one replica while a resident copy is selected on another.
        let candidates = cold
            .index
            .instances_for_job_type_rotated(job_type, None, usize::MAX);
        for key in candidates {
            match self.rehydrate_cold(key) {
                ColdRehydration::Restored => {}
                ColdRehydration::Unreadable => {
                    return Err(EngineError::InstanceUnavailable { instance_key: key });
                }
                ColdRehydration::NotCold | ColdRehydration::Absent => {}
            }
        }
        Ok(())
    }

    fn rehydrate_activation_candidates(&mut self, job_type: &str, max_jobs: usize) {
        // Rehydrate up to `max_jobs` cold instances holding an activatable job of
        // this type, so a worker poll can reach a parked-then-cold backlog. Only
        // pays a SQLite read when something of this type is actually cold.
        //
        // Selection rotates past the last unreadable row (#1388) under a bounded
        // read budget, so a permanently unreadable oldest row (kept cold and
        // routable) cannot starve healthy later instances of the same type.
        let Some(cold) = self.cold.as_ref() else {
            return;
        };
        let budget = max_jobs.saturating_add(ACTIVATION_EXTRA_READS);
        let after = self.activation_cursor.get(job_type).copied();
        let candidates = cold
            .index
            .instances_for_job_type_rotated(job_type, after, budget);
        let mut restored = 0usize;
        for key in candidates {
            if restored >= max_jobs {
                break;
            }
            match self.rehydrate_cold(key) {
                ColdRehydration::Restored => restored += 1,
                // Unreadable rows stay cold; their jobs are simply not offered
                // this poll, and the next poll starts after them.
                ColdRehydration::Unreadable => {
                    self.activation_cursor.insert(job_type.to_string(), key);
                }
                ColdRehydration::NotCold | ColdRehydration::Absent => {}
            }
        }
    }

    /// Rehydrates the spilled variables every activated job needs (#1331), and
    /// drops from `activated` any job whose instance could not be rehydrated
    /// (#1388) — a job is never handed out with the empty spill placeholder as
    /// its variables. In lean mode the authoritative var store is read
    /// non-destructively (the row must survive for the next recovery); classic
    /// mode reads the spill cache non-destructively, restores, then forgets the
    /// row.
    ///
    /// - **Unreadable**: logged at ERROR, counted, the row kept, the instance
    ///   left spilled. The job stays activated in the engine (its lease expires
    ///   and it becomes activatable again), so the activation is retried.
    /// - **Definitively absent**: the payload is lost; the instance is
    ///   quarantined via the #1383 orphan path (spilled flag cleared, surviving
    ///   resident variables kept) and the job proceeds with those.
    fn rehydrate_activated_variables(&mut self, activated: &mut Vec<ActivatedJob>) {
        if self.varstore.is_none() && self.spill.is_none() {
            return;
        }
        // A single spilled instance can have *several* jobs activated in the
        // same batch (parallel gateway / multi-instance). Rehydrate each unique
        // instance exactly once, but refresh the variable projection for EVERY
        // job of a rehydrated instance (#1331).
        // Pass 1: attempt restoration once per instance. Pass 2: refresh EVERY
        // retained job's projection, so a job whose instance was restored by a
        // later attempt in the batch never keeps the empty placeholder (#1388).
        let mut attempted: HashSet<Key> = HashSet::new();
        let mut rehydrated: HashSet<Key> = HashSet::new();
        let targets: Vec<(Key, Key)> = activated.iter().map(|j| (j.instance_key, j.key)).collect();
        for (instance_key, job_key) in targets {
            if attempted.insert(instance_key) && self.engine.is_variables_spilled(instance_key) {
                self.rehydrate_spilled_variables(instance_key, job_key);
                if !self.engine.is_variables_spilled(instance_key) {
                    rehydrated.insert(instance_key);
                }
            }
        }
        for job in activated.iter_mut() {
            if rehydrated.contains(&job.instance_key) {
                // Root is resident again; recompute the merged scoped view so a
                // job on a nested scope regains its scope-local bindings.
                job.variables = self
                    .engine
                    .element_variables(job.instance_key, job.element_instance_key);
            }
        }
        activated.retain(|job| !self.engine.is_variables_spilled(job.instance_key));
    }

    /// Restores `instance_key`'s spilled variables into the engine (see
    /// [`rehydrate_activated_variables`](Journal::rehydrate_activated_variables)).
    /// On an unreadable row the instance is left spilled.
    fn rehydrate_spilled_variables(&mut self, instance_key: Key, job_key: Key) {
        if let Err(err) = self.restore_spilled_variables(instance_key, "activation") {
            self.offheap_read_failures.fetch_add(1, Ordering::Relaxed);
            tracing::error!(
                instance_key,
                job_key,
                error = %err,
                "activation: spilled instance's variable row could not be read; the \
                 job is withheld from the worker (row kept, retried when its lease \
                 expires)"
            );
        }
    }

    /// The single spilled-variable restore (#1388/#1389): a non-destructive
    /// legacy-aware read; on success the variables are rehydrated and (classic
    /// mode) only then the dead cache row forgotten; a definitively absent row
    /// is quarantined as an orphan (the instance keeps its surviving resident
    /// variables). `Err` = the row is unreadable: nothing is changed, the caller
    /// fails closed. `context` prefixes the log lines. No-op when no tier is
    /// wired.
    fn restore_spilled_variables(
        &mut self,
        instance_key: Key,
        context: &str,
    ) -> Result<(), String> {
        let lean = self.varstore.is_some();
        if !lean && self.spill.is_none() {
            return Ok(());
        }
        // Legacy-aware (#1388): a lean miss is only "absent" once the legacy
        // tier is wired and also has no row; otherwise it fails closed.
        match self.peek_spilled_variables(instance_key)? {
            Some(vars) => {
                self.engine
                    .rehydrate_variables(instance_key, Arc::new(vars));
                if !lean {
                    // Classic: the payload is resident again — only now is the
                    // destructive cache row dead (#1388).
                    if let Some(Err(err)) = self
                        .spill
                        .as_ref()
                        .map(|vs| vs.store.forget(&[instance_key]))
                    {
                        tracing::error!(
                            instance_key,
                            error = %err,
                            "{context}: variables restored but the spill row could \
                             not be forgotten (latched for retry)"
                        );
                    }
                }
            }
            None => {
                self.note_orphan(
                    instance_key,
                    &format!(
                        "{context}: spilled instance's variable row is definitively absent \
                         from a readable store; its spilled variables are lost — quarantined \
                         (continuing with its surviving resident variables)"
                    ),
                );
                let resident = self
                    .engine
                    .instance(instance_key)
                    .map(|i| Arc::clone(&i.variables))
                    .unwrap_or_default();
                self.engine.rehydrate_variables(instance_key, resident);
                // Under replicated (Raft) application this path runs inside the
                // state machine: raising the incident here would mint a
                // replica-local key and stamp wall-clock time, diverging replicas
                // whose spill state differs. Observe only (#1384; the
                // leader-propose redesign is tracked separately).
                if self.replicated_apply {
                    crate::metrics::record_offheap_orphan_unreported();
                    tracing::error!(
                        instance_key,
                        "{context}: off-heap orphan quarantined during replicated \
                         apply; VariablesLost incident NOT raised (replica \
                         determinism) — observed only"
                    );
                    return Ok(());
                }
                // Raise the durable incident (#1384) — idempotent, so
                // a repeated activation/reconcile does not double-raise.
                // Raised WITHOUT the per-command spill housekeeping: the
                // caller (an outer command or activation) is about to consume
                // the surviving resident variables, and a due spill check with
                // an exhausted budget would re-spill the orphan first
                // (Copilot review on #1442).
                self.raise_quarantine_incident(instance_key, context);
            }
        }
        Ok(())
    }

    /// Fires every due timer at logical instant `now`, journaling the resulting
    /// events (timers are durable business facts). Mirrors
    /// [`Engine::trigger_timers`]; returns the events produced (empty if none
    /// were due) and their [`Commit`].
    pub fn trigger_timers(&mut self, now: u64) -> (Arc<Vec<Event>>, Commit) {
        if self.fenced {
            return (Arc::new(Vec::new()), Commit::ready());
        }
        // Rehydrate any cold instance whose armed timer is now due, so the tick
        // can fire it. No-op when nothing cold is due.
        if let Some(cold) = self.cold.as_ref() {
            let due = cold.index.instances_due(now);
            for key in due {
                // An unreadable row stays cold and armed; the next tick retries.
                let _ = self.rehydrate_cold(key);
            }
        }
        let events = Arc::new(self.engine.trigger_timers(now));
        let commit = self.persist(&events);
        (events, commit)
    }

    /// Whether any timer — hot or cold — is due at or before `now`. The Raft
    /// tick pre-check (#1483) gates proposing `TriggerTimers` on this: gating
    /// on hot timers alone strands a partition whose every due timer is
    /// cold-spilled (the command that would rehydrate and fire them is never
    /// proposed), so the check must cover the cold routing index too.
    pub fn has_due_timers(&self, now: u64) -> bool {
        // Mirror the engine's own due filter (`TriggerTimers` fires only
        // `Created` timers — fired ones are retained so they never re-fire).
        let hot_due = self
            .engine
            .state()
            .timers
            .values()
            .any(|t| t.state == nanobpmn_engine_core::TimerState::Created && t.due_at <= now);
        hot_due
            || self
                .cold
                .as_ref()
                .is_some_and(|cold| cold.index.has_due_timers(now))
    }

    /// Releases only token-free, volatile activation locks without journaling.
    /// Token-bearing leases must expire through a durable command.
    pub fn expire_jobs(&mut self, now: u64) -> Vec<Event> {
        self.engine.expire_jobs_by_durability(now, false)
    }

    /// Read-only access to the underlying engine (for projections that take an
    /// `&Engine`).
    pub fn engine(&self) -> &Engine {
        &self.engine
    }

    /// Sets the cluster-wide partition count on the underlying engine so it
    /// places message subscriptions on the partition owning their correlation
    /// key (see [`Engine::set_num_partitions`]). A no-op effect when `1`.
    pub fn set_num_partitions(&mut self, num_partitions: u64) {
        self.engine.set_num_partitions(num_partitions);
    }

    /// Enables leftover-tolerant ("lenient") job completion on the underlying
    /// engine so a replicated `CompleteJob`/`FailJob`/`ThrowJobError` applies even
    /// when this engine never observed the (leader-local) activation. See
    /// [`Engine::set_lenient_completion`]. Set identically on every replica from
    /// `NANOBPMN_REPLICATE_ACTIVATION=0`.
    pub fn set_lenient_completion(&mut self, lenient: bool) {
        self.engine.set_lenient_completion(lenient);
    }

    /// Installs the shared [`ClusterVariables`] handle on this journal and its
    /// engine, so FEEL expressions evaluated during command processing resolve the
    /// host's live global + per-tenant cluster variables. The handle is retained so
    /// it can be re-installed after a snapshot restore (see
    /// [`restore_engine_from_snapshot`](Self::restore_engine_from_snapshot)).
    pub fn set_cluster_variables(&mut self, cluster_variables: ClusterVariables) {
        self.cluster_variables = cluster_variables.clone();
        self.engine.set_cluster_variables(cluster_variables);
    }

    /// The currently-held activation leases `(job_key, deadline)` on the
    /// underlying engine — the body of a best-effort lease digest broadcast (see
    /// [`Engine::activated_leases`]). Pure read.
    pub fn activated_leases(&self) -> Vec<(u64, u64)> {
        self.engine.activated_leases()
    }

    /// Recovers a soft activation lease from a digest on the underlying engine
    /// (Created → Activated until `deadline`). Soft state only — journals nothing.
    /// See [`Engine::recover_lease`].
    pub fn recover_lease(
        &mut self,
        job_key: u64,
        deadline: u64,
        worker: Option<&str>,
        now: u64,
    ) -> bool {
        // A promoted follower may hold this job only in its cold index (#963):
        // page the owning instance in first, or the lease — and the digest's
        // worker attribution — would be silently missed. Best-effort, like the
        // digest itself: an unreadable row stays cold and is retried next tick.
        // An already-expired lease is rejected *before* paging anything in —
        // the engine would refuse it anyway, and rehydrating first would churn
        // a cold instance in and out on every digest tick until the longest
        // lease in the digest expires.
        if deadline <= now {
            return false;
        }
        if let Some(instance) = self
            .cold
            .as_ref()
            .and_then(|cold| cold.index.instance_for_job(job_key))
        {
            let _ = self.rehydrate_cold(instance);
        }
        self.engine.recover_lease(job_key, deadline, worker, now)
    }

    /// The activating worker of each soft lease (see
    /// [`Engine::activated_lease_workers`]), broadcast in the lease digest so
    /// a promoted leader restores the real attribution (#963).
    pub fn activated_lease_workers(&self) -> Vec<(u64, String)> {
        self.engine.activated_lease_workers()
    }

    // --- Read delegations mirroring the engine API the server uses. ---

    pub fn state(&self) -> &State {
        self.engine.state()
    }

    /// Captures a compact, serializable snapshot of the engine's live state (see
    /// [`Engine::snapshot`]) with its off-heap payloads folded back in — the
    /// **best-effort** form, for consumers that need a value regardless of fold
    /// completeness. It currently has only test callers; it is **not** the
    /// capture behind any durable recovery point: both of those — the
    /// compaction-gating classic rotation
    /// ([`snapshot_and_rotate`](Journal::snapshot_and_rotate)) and the Raft
    /// snapshot builder (`get_snapshot_builder`, after which openraft may purge
    /// the covered log) — use the fail-closed [`try_engine_snapshot`] instead, so
    /// an incomplete snapshot is never sealed or published. (The Raft catch-up /
    /// snapshot-serving path likewise never calls this: `get_current_snapshot`
    /// serves the **stored** snapshot that `get_snapshot_builder` captured
    /// fail-closed.) This wrapper ignores a
    /// failed fold (an unreadable/absent off-heap payload is logged and left
    /// as-is, see `fold_offheap_into`), so it can return a spilled placeholder or
    /// omit a later cold row; do not persist its result as a recovery point.
    ///
    /// Spill is a live-memory artefact and must never leak into a snapshot
    /// (#1331): a spilled instance holds only an empty placeholder in hot state,
    /// and a cold instance is absent from it entirely. Both are folded back in
    /// from the spill tiers here, by **non-destructive** reads, so the live
    /// instances stay off-heap while the snapshot restores every variable and
    /// every instance with no dependency on the spill store.
    ///
    /// **Memory (#1337, #1463):** neither snapshot path materializes the whole
    /// cold set any more. The Raft path captures the streaming halves
    /// ([`streaming_snapshot_parts`](Journal::streaming_snapshot_parts)) and
    /// `build_snapshot` streams the cold/spilled rows straight from the spill
    /// store to disk in bounded chunks; the local (non-Raft) rotation uses
    /// [`streaming_snapshot_and_rotate`](Journal::streaming_snapshot_and_rotate)
    /// and
    /// [`seglog::write_snapshot_streaming`](crate::seglog::write_snapshot_streaming),
    /// which seal the same chunked sidecar framing (snapshot format v3). This
    /// best-effort value wrapper still folds, but it backs no durable recovery
    /// point.
    pub fn engine_snapshot(&self) -> nanobpmn_engine_core::EngineSnapshot {
        let mut snap = self.engine.snapshot();
        // Best-effort (see the doc above): a failed fold is logged and left
        // as-is; durable recovery points use `try_engine_snapshot` instead.
        let _ = self.fold_offheap_into(&mut snap, OrphanPolicy::Strict);
        snap
    }

    /// The fail-closed counterpart of [`engine_snapshot`](Journal::engine_snapshot):
    /// `Some(snapshot)` only when **every** indexed off-heap payload (each cold
    /// instance, each spilled variable map) folded in successfully, `None` if any
    /// read **failed** (an unreadable store/row). A *definitively absent* row is
    /// not a failure: its payload is already lost, so the instance is quarantined
    /// and the snapshot proceeds (#1383). Snapshot rotation + compaction
    /// ([`snapshot_and_rotate`](Journal::snapshot_and_rotate)) gates on this: an
    /// incomplete snapshot must never be sealed and written, or compacting the
    /// covered journal prefix would permanently discard the state the fold could
    /// not read (a transient SQLite/read/deserialization failure turned into
    /// durable data loss). Returning `None` aborts the rotation before the seal,
    /// so no segment becomes eligible for compaction and the next tick retries.
    ///
    /// **Strict** (Copilot review on #1387): a definitively absent row also
    /// fails the capture here. This is the Raft snapshot builder's capture
    /// (`get_snapshot_builder`): a Raft snapshot lets openraft purge the covered
    /// log and is shipped to peers, so a replica-local cache miss must never be
    /// turned into replicated state loss without a replicated transition
    /// recording it. Only the local journal rotation tolerates orphans — see
    /// [`try_engine_snapshot_quarantining`](Journal::try_engine_snapshot_quarantining).
    pub fn try_engine_snapshot(&self) -> Option<nanobpmn_engine_core::EngineSnapshot> {
        if self.fenced {
            return None;
        }
        let mut snap = self.engine.snapshot();
        self.fold_offheap_into(&mut snap, OrphanPolicy::Strict)?;
        Some(snap)
    }

    /// The local-journal-rotation capture (#1383): like
    /// [`try_engine_snapshot`](Journal::try_engine_snapshot), fail-closed on an
    /// *unreadable* store, but a row that is **definitively absent** from a
    /// readable, wired store is quarantined (logged once, see `note_orphan`) so a
    /// permanently lost payload cannot block local snapshots + compaction forever.
    pub fn try_engine_snapshot_quarantining(&self) -> Option<nanobpmn_engine_core::EngineSnapshot> {
        if self.fenced {
            return None;
        }
        let mut snap = self.engine.snapshot();
        self.fold_offheap_into(&mut snap, OrphanPolicy::Quarantine)?;
        Some(snap)
    }

    /// The sealing counterpart of
    /// [`try_engine_snapshot_quarantining`](Journal::try_engine_snapshot_quarantining)
    /// (#1384 review): identical capture, but a definitively-absent spill row is
    /// first routed through the durable [`Command::QuarantineOffheapOrphan`] so the
    /// `VariablesLost` incident is **journaled** (and, under Raft, replicated)
    /// before the snapshot seals and the covered log compacts away. Without this a
    /// snapshot-first discovery cleared the spilled flag and sealed the snapshot
    /// with no incident, so a restart restored the cleared flag and boot
    /// reconciliation never reported the loss. The command is idempotent, so a
    /// re-raised orphan (or one already incidented by boot reconciliation) is a
    /// no-op. `&mut self` because raising the incident mutates engine state.
    pub fn try_engine_snapshot_quarantining_with_incidents(
        &mut self,
    ) -> Option<nanobpmn_engine_core::EngineSnapshot> {
        if self.fenced {
            return None;
        }
        // Under replicated (Raft) application the classic rotation still runs on
        // the Raft-owned actor, but raising an incident here would mint a
        // replica-local key + wall-clock stamp outside any Raft proposal and
        // diverge replicas (#1384 review). Capture STRICT there instead: an
        // orphan aborts the rotation, exactly like the Raft snapshot builder,
        // until the leader-propose redesign (#1447) lands.
        if self.replicated_apply {
            return self.try_engine_snapshot();
        }
        let mut snap = self.engine.snapshot();
        let orphans = self.fold_offheap_into(&mut snap, OrphanPolicy::Quarantine)?;
        if orphans.is_empty() {
            return Some(snap);
        }
        // Release the first capture BEFORE raising the incidents and
        // recapturing: it owns every off-heap payload the fold just
        // materialized, and the recapture below reads and allocates them all
        // again — holding both at once would double the off-heap working set
        // on the memory-constrained nodes spilling targets (Copilot review on
        // #1442).
        drop(snap);
        // Only keys whose row was DEFINITIVELY ABSENT (the fold's `Ok(None)`
        // arm) are orphans — a healthy fold also clears the snapshot copy's
        // spilled flag, so the flag pair cannot identify orphans (#1384 review).
        for key in orphans {
            // Clear the LIVE spilled flag too, keeping the surviving resident
            // map (as boot reconciliation does), so resolving the incident does
            // not leave the instance spilled and re-raise on the next snapshot.
            let resident = self
                .engine
                .instance(key)
                .map(|i| Arc::clone(&i.variables))
                .unwrap_or_default();
            self.engine.rehydrate_variables(key, resident);
            // Idempotent: an orphan already carrying an active incident is a no-op.
            self.raise_quarantine_incident(key, "snapshot");
        }
        // Re-capture so the sealed snapshot CARRIES the incident(s) just raised.
        // The orphans are no longer spilled, so this cannot re-quarantine them.
        self.try_engine_snapshot_quarantining()
    }

    /// Raises the durable [`Command::QuarantineOffheapOrphan`] incident for `key`
    /// and journals it, WITHOUT the per-command spill housekeeping
    /// (`maybe_spill`) the full [`apply_command_at`](Journal::apply_command_at)
    /// path runs (#1384 review). Every quarantine site has just restored the
    /// orphan's surviving resident variables for a consumer that needs them
    /// next — the outer Modify/Migrate command, the activated job, or the
    /// snapshot recapture — and a spill check that is due with an exhausted
    /// budget would immediately re-spill the orphan, failing that outer command
    /// with `InstanceUnavailable` or filtering its job out of activation. The
    /// incident command emits only `IncidentRaised`, so the activated-job
    /// rehydration step is a no-op here; spill housekeeping is left to the
    /// outer command that follows.
    fn raise_quarantine_incident(&mut self, key: Key, context: &str) {
        match self.engine.apply_command_at(
            Command::QuarantineOffheapOrphan { instance_key: key },
            now_millis(),
        ) {
            Ok(events) => {
                // The `Commit` is the journal write's durability receipt; the
                // incident must survive a crash, so keep the must-use explicit.
                let _commit = self.persist(&Arc::new(events));
            }
            Err(error) => {
                tracing::error!(
                    instance_key = key,
                    error = %error,
                    "{context}: failed to raise the VariablesLost incident for a \
                     quarantined orphan (will retry on the next quarantine path)"
                );
            }
        }
    }

    /// Folds cold instances and spilled variable payloads into `snap` (see
    /// [`engine_snapshot`](Journal::engine_snapshot)). Fail-closed: returns `None`
    /// the moment any indexed payload cannot be folded in (a cold row that will
    /// not read, or a spilled variable map whose row will not read), so a
    /// caller that will go on to seal + compact can abort instead of persisting an
    /// incomplete snapshot. A row that is *definitively absent* from a readable
    /// store is instead quarantined (#1383, see `note_orphan`). The `engine_snapshot` wrapper ignores the `None` (a
    /// payload that cannot be found is logged as an error and left as-is rather
    /// than silently serialized as an empty map); `try_engine_snapshot` honours it.
    fn fold_offheap_into(
        &self,
        snap: &mut nanobpmn_engine_core::EngineSnapshot,
        policy: OrphanPolicy,
    ) -> Option<Vec<Key>> {
        let mut orphans = Vec::new();
        self.begin_fold();
        // Cold rows carry their variables inline (a variable-spilled instance is
        // never cold-shed — `Engine::snapshot_instance` refuses it), so the classic
        // fold keeps them as-is; the loop below only resolves *resident*
        // variable-spilled instances.
        self.fold_cold_into(&mut snap.state, policy)?;
        for instance in snap.state.instances.values_mut() {
            if !instance.variables_spilled {
                continue;
            }
            match self.peek_spilled_variables(instance.key) {
                Ok(Some(vars)) => {
                    instance.variables = Arc::new(vars);
                    instance.variables_spilled = false;
                }
                // Definitive absence (#1383): the store read fine and has no row,
                // so retrying can never succeed. The variables are already lost;
                // blocking the snapshot would only stall compaction for every
                // other instance forever. Quarantine: clear the spilled flag and
                // KEEP the resident map — a legacy spilled instance may already
                // hold values merged in from journal-tail variable updates, and
                // those survive (Copilot review on #1387). Strict captures (Raft)
                // fail closed instead.
                Ok(None) if policy == OrphanPolicy::Quarantine => {
                    self.note_orphan(
                        instance.key,
                        "snapshot: spilled instance has no variable row in the spill store; \
                         its spilled variables are lost — quarantined (snapshotted with its \
                         surviving resident variables) so snapshots and compaction are not \
                         blocked",
                    );
                    instance.variables_spilled = false;
                    orphans.push(instance.key);
                }
                Ok(None) => {
                    self.note_blocker(
                        instance.key,
                        None,
                        "snapshot (strict): spilled instance has no variable row; aborting \
                         this snapshot",
                    );
                    return None;
                }
                // Unreadable (SQLite/IO/decode): possibly transient — fail closed
                // so the rotation aborts before the seal and the next tick retries.
                Err(err) => {
                    self.note_blocker(
                        instance.key,
                        Some(&err),
                        "snapshot: spilled instance's variable row could not be read; \
                         aborting this snapshot (will retry)",
                    );
                    return None;
                }
            }
        }
        self.fold_succeeded();
        Some(orphans)
    }

    /// Records `key` as a quarantined off-heap orphan, logging `msg` at ERROR
    /// only the first time it is seen (#1383 — the fold runs every snapshot
    /// tick, and an orphan is permanent).
    fn note_orphan(&self, key: Key, msg: &str) {
        let first = self
            .quarantined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key);
        if first {
            tracing::error!(instance_key = key, "{msg}");
        }
    }

    /// Starts a snapshot fold attempt: forgets the previous attempt's blockers
    /// (the log-dedupe set survives until a fold succeeds).
    fn begin_fold(&self) {
        self.snapshot_blockers
            .current
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// A fold completed: the outage (if any) is over, so a later recurrence of
    /// the same blocker is logged afresh.
    fn fold_succeeded(&self) {
        self.begin_fold();
        self.snapshot_blockers
            .logged
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clear();
    }

    /// Records `key` as blocking the current fold, logging `msg` at ERROR only
    /// the first time the key blocks during this outage (#1385); repeats are
    /// counted in [`snapshot_blocker_logs_suppressed`](Journal::snapshot_blocker_logs_suppressed).
    fn note_blocker(&self, key: Key, err: Option<&str>, msg: &str) {
        self.snapshot_blockers
            .current
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .push(key);
        let first = self
            .snapshot_blockers
            .logged
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .insert(key);
        if first {
            match err {
                Some(error) => tracing::error!(instance_key = key, error, "{msg}"),
                None => tracing::error!(instance_key = key, "{msg}"),
            }
        } else {
            self.snapshot_blockers
                .suppressed
                .fetch_add(1, Ordering::Relaxed);
            tracing::debug!(instance_key = key, "{msg} (repeat)");
        }
    }

    /// Instance keys whose off-heap payload aborted the most recent snapshot
    /// fold (#1385); empty after a successful fold.
    pub fn snapshot_blockers(&self) -> Vec<Key> {
        self.snapshot_blockers
            .current
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Repeat snapshot-blocker ERROR lines suppressed so far (#1385).
    pub fn snapshot_blocker_logs_suppressed(&self) -> u64 {
        self.snapshot_blockers.suppressed.load(Ordering::Relaxed)
    }

    /// Number of off-heap rehydration reads that failed as unreadable so far
    /// (#1388) — the row was kept and the read will be retried.
    pub fn offheap_read_failures(&self) -> u64 {
        self.offheap_read_failures.load(Ordering::Relaxed)
    }

    /// The worker-facing [`ActivatedJob`] for `job_key` (#1388): `None` while the
    /// owning instance's variables are still spilled off-heap, so a job is never
    /// handed to a worker with the empty spill placeholder as its variables.
    /// The instance is rehydrated at this boundary first: `apply_command_at`
    /// may have re-spilled it right after activation (`maybe_spill`), and a
    /// healthy activation must not be withheld for that.
    pub fn resident_activated_job(&mut self, job_key: Key) -> Option<ActivatedJob> {
        if self.fenced {
            return None; // see `activate_jobs_with_fetch` (#1405)
        }
        let mut job = self.engine.activated_job(job_key)?;
        if self.engine.is_variables_spilled(job.instance_key)
            && (self.varstore.is_some() || self.spill.is_some())
        {
            self.rehydrate_spilled_variables(job.instance_key, job_key);
            if !self.engine.is_variables_spilled(job.instance_key) {
                job.variables = self
                    .engine
                    .element_variables(job.instance_key, job.element_instance_key);
            }
        }
        if self.engine.is_variables_spilled(job.instance_key) {
            tracing::error!(
                job_key,
                instance_key = job.instance_key,
                "activation: withholding job whose instance variables are still spilled"
            );
            return None;
        }
        Some(job)
    }

    /// Instance keys quarantined as off-heap orphans so far (#1383), sorted.
    pub fn quarantined_orphans(&self) -> Vec<Key> {
        let mut keys: Vec<Key> = self
            .quarantined
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .iter()
            .copied()
            .collect();
        keys.sort_unstable();
        keys
    }

    /// Boot-time reconciliation (#1383): checks every variable-spilled instance
    /// in hot state against the off-heap stores and quarantines each one whose
    /// row is **definitively absent** — clearing its spilled flag (it becomes
    /// resident with an empty variable set; the payload was already lost, e.g.
    /// state carried over from a pre-#1331 snapshot whose spill store was wiped).
    /// Logs a single summary line and returns the quarantined keys. An
    /// *unreadable* store is left alone (the snapshot fold fails closed on it and
    /// retries). Run once after recovery, with the spill tiers wired.
    pub fn reconcile_offheap_orphans(&mut self) -> Vec<Key> {
        let spilled: Vec<Key> = self
            .engine
            .state()
            .instances
            .values()
            .filter(|i| i.variables_spilled)
            .map(|i| i.key)
            .collect();
        let mut orphans = Vec::new();
        for key in spilled {
            if matches!(self.peek_spilled_variables(key), Ok(None)) {
                // Keep the resident map (it may hold journal-tail updates merged
                // in after the legacy snapshot) — only the spilled flag clears.
                let current = self
                    .engine
                    .state()
                    .instances
                    .get(&key)
                    .map(|i| Arc::clone(&i.variables))
                    .unwrap_or_default();
                self.engine.rehydrate_variables(key, current);
                self.quarantined
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .insert(key);
                // Raise the durable, replicated incident (#1384): route the
                // quarantine through the engine command path so the
                // `VariablesLost` incident is journaled (and, under Raft,
                // replicated) like any other — not just a local log line. The
                // command is idempotent, so a repeated reconcile after a crash
                // between the note and the journal write does not double-raise.
                // Raised WITHOUT the per-command spill housekeeping
                // (`maybe_spill`): reconcile has just restored the orphan's
                // surviving resident variables, and a due spill check with an
                // exhausted budget would immediately re-spill it (Copilot
                // review on #1442).
                self.raise_quarantine_incident(key, "boot reconciliation");
                orphans.push(key);
            }
        }
        if !orphans.is_empty() {
            orphans.sort_unstable();
            tracing::error!(
                count = orphans.len(),
                instance_keys = ?orphans,
                "boot reconciliation: quarantined spilled instance(s) whose variable row \
                 is missing from the spill store; their spilled variables are lost and \
                 they keep only their surviving resident variables"
            );
        }
        orphans
    }

    /// Folds every cold (wholly off-heap) instance's control state back into
    /// `state` as a resident instance, returning each folded instance's
    /// `(key, variables)`.
    ///
    /// Shared by the classic snapshot ([`fold_offheap_into`](Journal::fold_offheap_into))
    /// and the lean checkpoint ([`snapshot_and_rotate_lean`](Journal::snapshot_and_rotate_lean)):
    /// cold spill removes a *whole* instance from engine state and is configured
    /// independently of lean mode, and nothing rebuilds the cold table at boot
    /// (the in-RAM index is populated only by in-process shedding), so **every**
    /// snapshot path must fold cold instances back in or they silently vanish on
    /// the next restart — the #1331 cold-tier defect. Fail-closed: a cold row that
    /// cannot be read is logged and the whole fold returns `None`, so a caller
    /// that seals + compacts aborts rather than persisting a snapshot that omits a
    /// live instance. A cold row always carries its variables inline (a
    /// variable-spilled instance is never cold-shed —
    /// [`Engine::snapshot_instance`] refuses it), so the returned payloads are
    /// authoritative.
    fn fold_cold_into(
        &self,
        state: &mut nanobpmn_engine_core::State,
        policy: OrphanPolicy,
    ) -> Option<VarUpsertDelta> {
        let mut folded = Vec::new();
        // No cold tier configured is not a failure — nothing to fold, so succeed
        // with an empty delta. Only an indexed cold row that will not read fails
        // the fold (`None`).
        let Some(cold) = self.cold.as_ref() else {
            return Some(folded);
        };
        for key in cold.index.keys() {
            match cold.store.try_get_cold(key) {
                Ok(Some(instance)) => {
                    nanobpmn_engine_core::restore_instance_snapshot(state, instance);
                    if let Some(inst) = state.instances.get(&key) {
                        folded.push((key, Arc::clone(&inst.variables)));
                    }
                }
                // Definitive absence (#1383): the whole instance is already lost
                // and can never be folded in; quarantine it (log once) rather
                // than block every future snapshot and all compaction.
                Ok(None) if policy == OrphanPolicy::Quarantine => self.note_orphan(
                    key,
                    "snapshot: cold instance has no row in the spill store; it is lost \
                     — quarantined (omitted) so snapshots and compaction are not blocked",
                ),
                Ok(None) => {
                    self.note_blocker(
                        key,
                        None,
                        "snapshot (strict): cold instance has no row; aborting this snapshot",
                    );
                    return None;
                }
                // Unreadable: possibly transient — fail closed and retry.
                Err(err) => {
                    self.note_blocker(
                        key,
                        Some(&err.to_string()),
                        "snapshot: cold instance's row could not be read; aborting this \
                         snapshot (will retry)",
                    );
                    return None;
                }
            }
        }
        Some(folded)
    }

    /// Decomposes the live engine into the streaming halves and the store handles
    /// needed to stream them to disk — everything the caller needs to run the
    /// serialize **off** the engine thread (#1337). Runs on the engine thread (a
    /// consistent read of the live state + the cold routing index); the returned
    /// parts own only `Arc` store handles and the resident payloads, so the
    /// follow-up [`write_streaming_snapshot_from_stores`](crate::snapstream::write_streaming_snapshot_from_stores)
    /// is purely synchronous and never folds the whole cold set into one
    /// in-memory [`EngineSnapshot`].
    ///
    /// A variable-spilled resident instance keeps its placeholder here; its real
    /// payload is resolved from the spill tier as it streams out. A cold instance
    /// is not read at all here — only its key is captured, and the row is read
    /// lazily during the stream.
    ///
    /// Returns `None` while the journal is fenced after an aborted Raft entry
    /// (#1405): the live engine may hold the entry's partial, non-durable
    /// effects, so no capture of it may be persisted or shipped.
    pub fn streaming_snapshot_parts(
        &self,
    ) -> Option<(
        crate::snapstream::StreamingCapture,
        Vec<Key>,
        crate::snapstream::SpillStores,
    )> {
        if self.fenced {
            return None;
        }
        let full = self.engine.snapshot();
        // Move the captured state into decomposition (rather than cloning it):
        // `Engine::snapshot` already cloned the resident `State` off the live
        // engine, and `decompose_state_for_streaming` consumes it by value, so a
        // second clone here would keep an unnecessary extra copy of its maps
        // alive during decomposition — raising peak memory on exactly the
        // memory-constrained nodes this change targets (#1464 review).
        let (control_state, resident) = Engine::decompose_state_for_streaming(full.state);
        let control = nanobpmn_engine_core::EngineSnapshot {
            state: control_state,
            partition_id: full.partition_id,
            next_local: full.next_local,
            num_partitions: full.num_partitions,
            now: full.now,
            start_dispatch_rr: full.start_dispatch_rr,
            raft_applied: full.raft_applied,
        };
        let cold_keys: Vec<Key> = self
            .cold
            .as_ref()
            .map(|c| c.index.keys().collect())
            .unwrap_or_default();
        let stores = crate::snapstream::SpillStores {
            cold: self.cold.as_ref().map(|c| Arc::clone(&c.store)),
            spill: self.spill.as_ref().map(|s| Arc::clone(&s.store)),
            varstore: self.varstore.as_ref().map(Arc::clone),
        };
        Some((
            crate::snapstream::StreamingCapture { control, resident },
            cold_keys,
            stores,
        ))
    }

    /// Streaming/sidecar counterpart of [`try_engine_snapshot`](Journal::try_engine_snapshot)
    /// (#1337): captures a self-contained snapshot **without** materializing the
    /// whole cold/spilled set in one in-memory [`EngineSnapshot`], streaming it
    /// to `path` as bounded chunk frames. Fail-closed, exactly like
    /// [`try_engine_snapshot`](Journal::try_engine_snapshot): returns `Err` the
    /// moment any cold row or spilled payload cannot be read, and the partial
    /// file is removed, so an incomplete snapshot is never sealed.
    pub fn write_streaming_snapshot_to(
        &self,
        path: &std::path::Path,
        header_line: &str,
        covered_events: u64,
    ) -> std::io::Result<()> {
        let (capture, cold_keys, stores) = self.streaming_snapshot_parts().ok_or_else(|| {
            std::io::Error::other(
                "journal fenced after an aborted Raft entry; refusing to snapshot (#1405)",
            )
        })?;
        crate::snapstream::write_streaming_snapshot_from_stores(
            path,
            header_line,
            covered_events,
            // Not a seglog journal base: no rebase-floor identity (#1405).
            None,
            capture,
            &cold_keys,
            stores,
            false,
        )
    }

    /// Reads a cold instance's whole snapshot row without restoring it into the
    /// engine — the validation read for the streaming capture (#1463 review).
    /// `Ok(Some)` the row is present and readable; `Ok(None)` it is definitively
    /// absent (the payload is already lost); `Err` the store/row is unreadable
    /// (possibly transient). Never becomes resident in engine state.
    fn peek_cold_instance(&self, key: Key) -> Result<Option<()>, String> {
        let Some(cold) = self.cold.as_ref() else {
            return Err("no cold store wired; cannot establish row presence".to_string());
        };
        cold.store
            .try_get_cold(key)
            .map(|opt| opt.map(|_| ()))
            .map_err(|e| e.to_string())
    }

    /// Reads a spilled instance's variables without consuming them: from the
    /// authoritative var store in lean mode, else from the spill cache.
    ///
    /// `Ok(None)` is a definitive absence — **only** a successful lookup in a
    /// wired, readable store establishes it. `Err` covers an unreadable
    /// store/row (#1383), **no store wired at all** (startup may continue
    /// without the spill tier, so an unwired store proves nothing about the row
    /// on disk), and a lean-mode miss whose legacy spill row still exists (a
    /// lean-over-legacy transition that has not migrated it yet) — all of which
    /// must fail closed rather than be quarantined (Copilot review on #1387).
    fn peek_spilled_variables(&self, key: Key) -> Result<Option<HashMap<String, Value>>, String> {
        let legacy = self
            .spill
            .as_ref()
            .map(|vs| &vs.store)
            .or(self.cold.as_ref().map(|c| &c.store));
        if let Some(vstore) = self.varstore.as_ref() {
            return match vstore.try_get(key).map_err(|e| e.to_string())? {
                Some(vars) => Ok(Some(vars)),
                // Don't restore the (possibly stale) legacy row; just refuse to
                // call the payload lost while it still exists.
                None => match legacy {
                    Some(store) => match store.try_get(key).map_err(|e| e.to_string())? {
                        Some(_) => Err("var store has no row but a legacy spill row exists \
                                        (unmigrated lean-over-legacy state)"
                            .to_string()),
                        None => Ok(None),
                    },
                    // No legacy tier to check: an empty var store cannot prove
                    // a lean-over-legacy placeholder's payload is gone — fail
                    // closed (Copilot review on #1387).
                    None => Err("var store has no row and no legacy spill tier is wired \
                                 to rule out an unmigrated legacy row"
                        .to_string()),
                },
            };
        }
        match legacy {
            Some(store) => store.try_get(key).map_err(|e| e.to_string()),
            None => Err("no spill store wired; cannot establish row absence".to_string()),
        }
    }

    /// Whether this journal is backed by a segmented (bounded-disk) log.
    pub fn is_segmented(&self) -> bool {
        self.seg.is_some()
    }

    /// This journal's live engine [`State`], as recovered at boot (snapshot +
    /// surviving-tail replay). Used to reproject a read model that has fallen
    /// below the journal compaction floor (issue #732).
    pub fn engine_state(&self) -> &nanobpmn_engine_core::State {
        self.engine.state()
    }

    /// The global partition id this journal's engine owns (0 for single-partition).
    pub fn partition_id(&self) -> u64 {
        self.partition_id
    }

    /// Captures the engine snapshot and seals (rotates) the active segment at the
    /// exact boundary the snapshot covers, returning `(snapshot, covered_events)`
    /// — the absolute count of events the snapshot subsumes. `None` for a journal
    /// that is not segmented (or whose writer is gone).
    ///
    /// Must run **on the engine actor thread** (e.g. via `DeepthiHandle::with`):
    /// the snapshot reflects every command applied so far, and routing the rotate
    /// through the same ordered writer channel — after the writes those commands
    /// already enqueued — makes the sealed boundary line up exactly with the
    /// snapshot. No command runs between the snapshot and the seal because the
    /// actor is single-threaded.
    pub fn snapshot_and_rotate(&mut self) -> Option<(nanobpmn_engine_core::EngineSnapshot, u64)> {
        if self.fenced {
            return None;
        }
        self.seg.as_ref()?;
        // Fail-closed: abort the rotation (returning `None` before the seal) if any
        // off-heap payload could not be folded in, so an incomplete snapshot is
        // never sealed/written and its covered prefix never becomes eligible for
        // compaction — the caller skips this tick and retries. A definitively
        // absent spill row is quarantined AND its `VariablesLost` incident is
        // journaled before the seal (#1384 review), so the loss survives restart.
        // The capture borrows `self` mutably, so it runs before the `writer` borrow.
        let snap = self.try_engine_snapshot_quarantining_with_incidents()?;
        self.rotate_sealing().map(|covered| (snap, covered))
    }

    /// The streaming/sidecar counterpart of
    /// [`snapshot_and_rotate`](Journal::snapshot_and_rotate) (#1463): captures the
    /// same consistent engine state and seals the active segment at the same
    /// boundary, but returns the **decomposed streaming parts**
    /// ([`streaming_snapshot_parts`](Journal::streaming_snapshot_parts)) instead
    /// of one folded in-memory [`EngineSnapshot`]. The caller streams them to
    /// disk off the engine thread with
    /// [`write_streaming_snapshot_from_stores`](crate::snapstream::write_streaming_snapshot_from_stores)
    /// (via [`seglog::write_snapshot_streaming`](crate::seglog::write_snapshot_streaming)),
    /// so the whole cold/spilled set is never resident at once on the local
    /// (non-Raft) snapshot path either.
    ///
    /// Fail-closed exactly like [`snapshot_and_rotate`](Journal::snapshot_and_rotate):
    /// every off-heap row is validated **before** the seal, but row-at-a-time and
    /// dropped immediately (never folded into one in-memory snapshot — that would
    /// recreate the whole-cold-set peak this path exists to avoid, #1463 review).
    /// A definitively-absent row is quarantined with its journaled
    /// `VariablesLost` incident before the seal (#1384), and a proven-absent COLD
    /// key is excluded from the returned `cold_keys` so the streaming writer does
    /// not reject the lost row on every tick (which would block snapshots and
    /// compaction forever).
    ///
    /// **Capture→write races (#1463 review):** the returned store handles are
    /// shared `Arc`s, so a command resuming between the capture and the off-thread
    /// write can rehydrate (forgetting a row) or re-spill (rewriting one). That
    /// race is **fail-closed, never a torn snapshot**: a row the writer can no
    /// longer read aborts the write and removes the partial file (exactly like an
    /// unreadable row), so an incomplete snapshot is never sealed and the covered
    /// prefix never compacts — the tick retries. A re-spilled row is rewritten
    /// under the same key with equal-or-newer state, so the write either resolves
    /// the current payload or fails closed; it never seals a stale one.
    pub fn streaming_snapshot_and_rotate(
        &mut self,
    ) -> Option<(
        crate::snapstream::StreamingCapture,
        Vec<Key>,
        crate::snapstream::SpillStores,
        u64,
    )> {
        // Fenced after an aborted Raft entry (#1405): refuse BEFORE validation
        // can journal anything or the seal can rotate.
        if self.fenced {
            return None;
        }
        self.seg.as_ref()?;
        // Validate + quarantine row-at-a-time (journaling any `VariablesLost`
        // incident BEFORE the seal), collecting the cold keys proven ABSENT so
        // they are excluded from the streaming write. No whole-set fold happens
        // here — each peeked row is dropped before the next is read.
        let absent_cold = self.validate_offheap_for_streaming()?;
        let (capture, cold_keys, stores) = self.streaming_snapshot_parts()?;
        let cold_keys: Vec<Key> = cold_keys
            .into_iter()
            .filter(|k| !absent_cold.contains(k))
            .collect();
        let covered = self.rotate_sealing()?;
        Some((capture, cold_keys, stores, covered))
    }

    /// The fail-closed, row-at-a-time off-heap validation behind
    /// [`streaming_snapshot_and_rotate`](Journal::streaming_snapshot_and_rotate)
    /// (#1463 review): the streaming counterpart of
    /// [`try_engine_snapshot_quarantining_with_incidents`](Journal::try_engine_snapshot_quarantining_with_incidents)
    /// that never materializes the whole cold/spilled set.
    ///
    /// Every resident spilled instance and every indexed cold row is peeked
    /// (read + dropped, never folded into one snapshot). An UNREADABLE row fails
    /// closed (`None` — abort before the seal, retry next tick). A DEFINITIVELY
    /// ABSENT row is quarantined and its `VariablesLost` incident journaled
    /// before the seal (local policy; under replicated apply the capture is
    /// STRICT — an orphan aborts the rotation, #1384). Returns the cold keys
    /// proven absent (to be excluded from the streaming write).
    fn validate_offheap_for_streaming(&mut self) -> Option<Vec<Key>> {
        use std::collections::HashSet;
        let strict = self.replicated_apply;
        let mut absent_cold: Vec<Key> = Vec::new();
        let mut orphans: HashSet<Key> = HashSet::new();

        self.begin_fold();

        // Resident spilled instances: peek each payload (dropped immediately).
        let spilled_keys: Vec<Key> = self
            .engine
            .state()
            .instances
            .values()
            .filter(|i| i.variables_spilled)
            .map(|i| i.key)
            .collect();
        for key in spilled_keys {
            match self.peek_spilled_variables(key) {
                Ok(Some(_)) => {}
                Ok(None) if !strict => {
                    self.note_orphan(
                        key,
                        "snapshot: spilled instance has no variable row in the spill store; \
                         its spilled variables are lost — quarantined (snapshotted with its \
                         surviving resident variables) so snapshots and compaction are not \
                         blocked",
                    );
                    orphans.insert(key);
                }
                Ok(None) => {
                    self.note_blocker(
                        key,
                        None,
                        "snapshot (strict): spilled instance has no variable row; aborting \
                         this snapshot",
                    );
                    return None;
                }
                Err(err) => {
                    self.note_blocker(
                        key,
                        Some(&err),
                        "snapshot: spilled instance's variable row could not be read; \
                         aborting this snapshot (will retry)",
                    );
                    return None;
                }
            }
        }

        // Cold rows: peek each (read + dropped, never restored into the engine).
        let cold_keys: Vec<Key> = self
            .cold
            .as_ref()
            .map(|c| c.index.keys().collect())
            .unwrap_or_default();
        for key in cold_keys {
            match self.peek_cold_instance(key) {
                Ok(Some(())) => {}
                Ok(None) if !strict => {
                    self.note_orphan(
                        key,
                        "snapshot: cold instance has no row in the spill store; it is lost \
                         — quarantined (omitted) so snapshots and compaction are not blocked",
                    );
                    absent_cold.push(key);
                    orphans.insert(key);
                }
                Ok(None) => {
                    self.note_blocker(
                        key,
                        None,
                        "snapshot (strict): cold instance has no row; aborting this snapshot",
                    );
                    return None;
                }
                Err(err) => {
                    self.note_blocker(
                        key,
                        Some(&err.to_string()),
                        "snapshot: cold instance's row could not be read; aborting this \
                         snapshot (will retry)",
                    );
                    return None;
                }
            }
        }
        self.fold_succeeded();

        // Journal the VariablesLost incident for each orphan BEFORE the seal
        // (#1384), mirroring `try_engine_snapshot_quarantining_with_incidents`.
        // Under replicated apply we returned None on the first orphan already.
        for &key in &orphans {
            // Clear the LIVE spilled flag for a resident orphan (a cold orphan is
            // off-heap already), keeping the surviving resident map, so resolving
            // the incident does not leave it spilled and re-raise next snapshot.
            let resident = self
                .engine
                .instance(key)
                .map(|i| Arc::clone(&i.variables))
                .unwrap_or_default();
            self.engine.rehydrate_variables(key, resident);
            self.raise_quarantine_incident(key, "snapshot");
        }
        Some(absent_cold)
    }

    /// The seal half of a snapshot rotation, shared by
    /// [`snapshot_and_rotate`](Journal::snapshot_and_rotate) and
    /// [`streaming_snapshot_and_rotate`](Journal::streaming_snapshot_and_rotate):
    /// routes the rotate through the ordered writer channel (after every write
    /// the captured state already reflects) and returns this partition's covered
    /// event count at the sealed boundary. `None` when the writer is gone.
    fn rotate_sealing(&mut self) -> Option<u64> {
        let writer = self.writer.as_ref()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        if writer.send(WriterMsg::Rotate(reply_tx)).is_err() {
            return None;
        }
        let info = reply_rx.blocking_recv().ok()?;
        // On the multi-partition path the seal carries per-partition boundaries;
        // this journal's covered count is its own partition's cumulative total.
        // On the single-partition path `per_partition_end` is empty and the whole
        // log belongs to partition 0, so `info.end` is the covered count.
        let covered = info
            .per_partition_end
            .get(self.partition_id as usize)
            .copied()
            .unwrap_or(info.end);
        Some(covered)
    }

    /// Makes the engine's CURRENT state this journal's durable recovery base
    /// (#1405): seals the active segment at the current boundary and persists a
    /// snapshot covering it (lean: the variable delta goes to the var store
    /// first), in the layout this journal recovers from (single-partition
    /// snapshot file, or this partition's entry in the combined multi-partition
    /// snapshot). Called right after the engine adopts a Raft snapshot that is
    /// NEWER than the journal: without it the journal would hold only the older
    /// history, and once later entries were journaled a restart preferring the
    /// journal would lose everything only the snapshot carried.
    ///
    /// An in-memory journal (no writer) has nothing durable to rebase and is a
    /// no-op. A durable journal that cannot hold a snapshot base (non-segmented),
    /// or a failed/refused checkpoint (fenced, unfoldable cold state), errors —
    /// the caller must fail closed rather than run on a journal that would
    /// silently lose state on the next restart.
    ///
    /// A segmented journal requires its directory's
    /// [`CheckpointPermit`](crate::seglog::CheckpointPermit) (else it errors): the whole cycle is serialized with the periodic
    /// checkpoint, so an older periodic capture cannot be published over the
    /// adopted base. A durable REBASE FLOOR is recorded before the base snapshot
    /// is published, so recovery fails closed (never replays a history lacking
    /// the adopted state) if that snapshot is later missing or unreadable.
    pub fn checkpoint_base(
        &mut self,
        permit: Option<&crate::seglog::CheckpointPermit>,
    ) -> io::Result<()> {
        if self.writer.is_none() {
            return Ok(());
        }
        let Some(seg) = self.seg.clone() else {
            return Err(io::Error::other(
                "a non-segmented (legacy single-file) durable journal cannot hold a Raft \
                 snapshot as its recovery base, so booting it over a newer snapshot would \
                 lose that snapshot's state on a later restart; re-enable the segmented \
                 journal (the default) for this replica (#1405)",
            ));
        };
        if !permit.is_some_and(|p| p.is_for(&seg)) {
            return Err(io::Error::other(
                "checkpoint_base requires this journal directory's checkpoint permit (#1405)",
            ));
        }
        let refused = || io::Error::other("journal checkpoint refused (fenced or incomplete)");
        let floor_partition = (!seg.per_partition_total.is_empty()).then_some(self.partition_id);
        // Publish the rebase floor BEFORE touching any authoritative state (the
        // var store, then the snapshot): a crash at any later boundary leaves a
        // floor no older snapshot satisfies, so recovery fails closed instead of
        // mixing newly adopted variables with the old control state (#1405).
        let snap = if let Some(vs) = self.varstore.clone() {
            let (snap, covered, upserts, forgets) =
                self.snapshot_and_rotate_lean().ok_or_else(refused)?;
            crate::seglog::write_rebase_floor(&seg.dir, floor_partition, covered)?;
            #[cfg(test)]
            if FAIL_AFTER_FLOOR.with(|f| f.get()) {
                return Err(io::Error::other("injected crash after the floor"));
            }
            let ups: Vec<(Key, &HashMap<String, Value>)> =
                upserts.iter().map(|(k, v)| (*k, v.as_ref())).collect();
            vs.checkpoint(self.partition_id, covered, &ups, &forgets)
                .map_err(io::Error::other)?;
            (snap, covered)
        } else {
            let (snap, covered) = self.snapshot_and_rotate().ok_or_else(refused)?;
            crate::seglog::write_rebase_floor(&seg.dir, floor_partition, covered)?;
            (snap, covered)
        };
        let (snap, covered) = snap;
        match floor_partition {
            None => crate::seglog::write_snapshot(&seg.dir, snap, covered),
            Some(p) => crate::seglog::upsert_multi_snapshot_entry(&seg.dir, p, covered, snap),
        }
    }

    /// This journal's segmented seal state, if segmented — the source of the
    /// [`CheckpointPermit`](crate::seglog::CheckpointPermit) that
    /// [`checkpoint_base`](Journal::checkpoint_base) requires.
    pub fn seg_shared(&self) -> Option<Arc<SegShared>> {
        self.seg.clone()
    }

    /// The lean-snapshot counterpart of
    /// [`snapshot_and_rotate`](Journal::snapshot_and_rotate): drains this
    /// partition's variable delta since the last checkpoint (the current maps of
    /// instances whose variables changed, plus the keys of instances that reached
    /// a terminal state), captures a **control-only** snapshot (empty variable
    /// maps), and seals the active segment at the covered boundary — all on the
    /// engine actor thread, so the delta, snapshot and seal describe the same
    /// point. The caller writes the delta to the durable var store and the lean
    /// snapshot to disk (both off the engine thread). Returns
    /// `(lean_snapshot, covered_events, upserts, forgets)`, or `None` if not
    /// segmented / the writer is gone. Only meaningful when a var store is wired
    /// ([`set_varstore`](Journal::set_varstore)); the drain is empty otherwise.
    ///
    /// Cold (wholly off-heap) instances are folded back in too: cold spill is
    /// configured independently of lean mode, so a cold instance is absent from
    /// both the control-only snapshot and the var store's working set, and would
    /// silently vanish on restart (the #1331 cold-tier defect). Its control state
    /// goes into the snapshot and its variables are routed through the upsert
    /// delta — keeping the snapshot variable-free while the var store stays the
    /// authoritative, consistent source for variables (no recovery rewind).
    /// Fail-closed: returns `None` (aborting before the seal) if a cold instance
    /// cannot be folded in, so an incomplete lean checkpoint never gates
    /// compaction.
    pub fn snapshot_and_rotate_lean(&mut self) -> Option<LeanCheckpoint> {
        if self.fenced {
            return None;
        }
        self.seg.as_ref()?;
        // Fold cold instances FIRST: it is a pure read of engine + cold store (no
        // mutation), so doing it before the drain is equivalent on the success
        // path, and on the fail-closed abort path it returns `None` WITHOUT having
        // drained the dirty-var delta — leaving the accumulated upserts/forgets
        // intact so the next checkpoint still captures them (draining first and
        // then aborting would silently drop them).
        let (snap, cold_upserts) = self.lean_snapshot_and_cold_upserts()?;
        // Drain BEFORE the seal so the delta reflects exactly the state the
        // control-only snapshot captures.
        let (mut upserts, forgets) = self.engine.drain_dirty_vars();
        upserts.extend(cold_upserts);
        let writer = self.writer.as_ref()?;
        let (reply_tx, reply_rx) = oneshot::channel();
        if writer.send(WriterMsg::Rotate(reply_tx)).is_err() {
            return None;
        }
        let info = reply_rx.blocking_recv().ok()?;
        let covered = info
            .per_partition_end
            .get(self.partition_id as usize)
            .copied()
            .unwrap_or(info.end);
        Some((snap, covered, upserts, forgets))
    }

    /// Builds the lean control-only snapshot together with the cold-instance
    /// variable upserts that must accompany it (see
    /// [`snapshot_and_rotate_lean`](Journal::snapshot_and_rotate_lean)). Folds
    /// each cold instance's control state into the snapshot and empties its
    /// variables there, returning those variables as upserts so the var store —
    /// authoritative in lean mode — is written in lockstep with the snapshot that
    /// omits them. Factored out so it is unit-testable without a segmented writer.
    /// Fail-closed: `None` if a cold instance cannot be folded in (see
    /// [`fold_cold_into`](Journal::fold_cold_into)).
    fn lean_snapshot_and_cold_upserts(
        &self,
    ) -> Option<(nanobpmn_engine_core::EngineSnapshot, VarUpsertDelta)> {
        let mut snap = self.engine.snapshot_control_only();
        self.begin_fold();
        let cold_upserts = self.fold_cold_into(&mut snap.state, OrphanPolicy::Quarantine)?;
        self.fold_succeeded();
        for (key, _) in &cold_upserts {
            if let Some(inst) = snap.state.instances.get_mut(key)
                && !inst.variables.is_empty()
            {
                inst.variables = Arc::new(HashMap::new());
            }
        }
        Some((snap, cold_upserts))
    }

    pub fn instance(&self, key: Key) -> Option<&ProcessInstance> {
        self.engine.instance(key)
    }

    pub fn incident(&self, key: Key) -> Option<&Incident> {
        self.engine.incident(key)
    }

    pub fn incidents(&self) -> Vec<&Incident> {
        self.engine.incidents()
    }
}

impl Drop for Journal {
    fn drop(&mut self) {
        // Close the channel so the writer thread drains any remaining requests
        // and exits, then join it to guarantee fire-and-forget writes hit disk.
        self.writer = None;
        if let Some(handle) = self.writer_thread.take() {
            let _ = handle.join();
        }
    }
}

#[cfg(test)]
mod tests {
    use nanobpmn_engine_core::ProcessBuilder;

    use super::*;

    /// Minimal no-runtime `block_on` for the commit handles (a `Commit` resolves
    /// on a plain `oneshot`, so a thread-parking waker suffices — no tokio
    /// reactor needed).
    fn block_on<F: std::future::Future>(fut: F) -> F::Output {
        use std::sync::mpsc;
        use std::task::{Context, Poll, Wake, Waker};

        struct Parker(mpsc::Sender<()>);
        impl Wake for Parker {
            fn wake(self: Arc<Self>) {
                let _ = self.0.send(());
            }
        }
        let (tx, rx) = mpsc::channel::<()>();
        let waker = Waker::from(Arc::new(Parker(tx)));
        let mut cx = Context::from_waker(&waker);
        let mut fut = std::pin::pin!(fut);
        loop {
            match fut.as_mut().poll(&mut cx) {
                Poll::Ready(out) => return out,
                Poll::Pending => rx.recv().unwrap(),
            }
        }
    }

    fn demo() -> nanobpmn_engine_core::ProcessDefinition {
        ProcessBuilder::new("demo")
            .start_event("start")
            .service_task("work", "demo-work")
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .expect("valid demo process")
    }

    /// Epic #1357 failure-injection guard: a poisoned shared-exporter-cell
    /// mutex must be RECOVERED (the guard is taken from the poison), never a
    /// panic — a poisoned cell must not take down the journal write path.
    #[test]
    fn poisoned_exporter_cell_lock_is_recovered_not_panicked() {
        let cell: ExporterCell =
            std::sync::Arc::new(std::sync::Mutex::new(std::collections::HashMap::new()));

        // Poison the mutex: panic while holding the guard.
        let poison_target = std::sync::Arc::clone(&cell);
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = poison_target.lock().unwrap();
            panic!("deliberate poison for the recovery test");
        }));
        std::panic::set_hook(prev);
        assert!(poisoned.is_err(), "the poisoning panic must propagate");
        assert!(
            cell.lock().is_err(),
            "the mutex is now poisoned (test precondition)"
        );

        // Exercise BOTH poisoned-lock acquisitions, not just `set_exporter`:
        // register partition 0 (insert-side recovery), then forward a NONEMPTY
        // batch (write-side recovery) and assert the batch is delivered and the
        // queued-byte gauge is updated. Passing `None`/an empty batch would make
        // `forward_shared_export` return before it ever acquires the poisoned
        // mutex, so the write-side recovery would go untested and a regression to
        // `.unwrap()` there would still pass (#1366).
        let (tx, rx) = std::sync::mpsc::channel();
        let queued = std::sync::Arc::new(AtomicU64::new(0));
        let mut journal = Journal::in_memory();
        journal.shared_exporter = Some(cell.clone());
        journal.set_exporter(tx, std::sync::Arc::clone(&queued));

        let events = std::sync::Arc::new(vec![Event::ProcessInstanceCompleted { instance_key: 7 }]);
        let bytes: u32 = 42;
        forward_shared_export(&Some(cell), 0, Some(std::sync::Arc::clone(&events)), bytes);

        let batch = rx
            .try_recv()
            .expect("batch delivered through the recovered (poisoned) cell");
        assert_eq!(batch.bytes, bytes, "forwarded byte count");
        assert_eq!(batch.events.len(), events.len(), "forwarded events");
        assert_eq!(
            queued.load(Ordering::Relaxed),
            bytes as u64,
            "queued-byte gauge accounts the forwarded batch",
        );
    }

    /// #1473 (review): `replace_engine` installs an engine rebuilt from durable
    /// state, whose replay resurrects every completed instance whose terminal
    /// event survived. Those events are NOT re-exported, so the exporter never
    /// drives another eviction for them — the swap must evict the rebuilt
    /// terminal shells itself, or the rebuilt engine pins its whole completed
    /// history resident indefinitely.
    #[test]
    fn replace_engine_evicts_the_rebuilt_completed_instances() {
        // Build the durable history of an instance that runs to COMPLETION.
        let mut history: Vec<Event> = Vec::new();
        let mut source = Journal::in_memory();
        let (d, _) = source
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        history.extend(d.iter().cloned());
        let (c, _) = source
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        let key = c.iter().find_map(|e| e.instance_key()).unwrap();
        history.extend(c.iter().cloned());
        let jobs = source.activate_jobs("demo-work", "w", 1, 60_000, 0);
        assert_eq!(jobs.len(), 1);
        let (done, _) = source
            .apply_command(Command::complete_job(jobs[0].key))
            .unwrap();
        assert!(done.iter().any(
            |e| matches!(e, Event::ProcessInstanceCompleted { instance_key } if *instance_key == key)
        ));
        history.extend(done.iter().cloned());
        assert!(
            source.instance(key).is_some(),
            "the completed shell is resident"
        );

        // The durable replay of that history resurrects the completed shell.
        let replayed = nanobpmn_engine_core::Engine::replay_partition(0, history);
        assert!(
            replayed.state().instances.contains_key(&key),
            "the replay resurrects the completed instance"
        );

        // Swapping it in must evict the terminal shell (not leave it resident).
        let mut target = Journal::in_memory();
        target.replace_engine(replayed);
        assert!(
            target.instance(key).is_none(),
            "replace_engine evicts the rebuilt terminal shell"
        );
    }

    #[test]
    fn volatile_expiry_never_mutates_durable_leases() {
        let mut journal = Journal::in_memory();
        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        for _ in 0..2 {
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
        }
        let (events, _) = journal
            .apply_command_at(
                Command::activate_jobs_with_options(
                    "demo-work",
                    "leased",
                    1,
                    10,
                    0,
                    nanobpmn_engine_core::JobActivationOptions {
                        with_lease: true,
                        ..Default::default()
                    },
                ),
                0,
            )
            .unwrap();
        let leased = events
            .iter()
            .find_map(|event| match event {
                Event::JobActivated { job_key, .. } => Some(*job_key),
                _ => None,
            })
            .unwrap();
        let soft = journal.activate_jobs("demo-work", "soft", 1, 10, 0)[0].key;
        let expired = journal.expire_jobs(10);
        assert_eq!(expired.len(), 1);
        assert!(journal.state().jobs[&leased].deadline.is_some());
        assert!(journal.state().jobs[&soft].deadline.is_none());
    }

    /// #1181 regression: an eventless success (an idempotent settlement replay)
    /// must NOT be acknowledged on a bare `Commit::ready()` from a file-backed
    /// journal while the ORIGINAL settlement could still be in the writer's
    /// queue — a crash in between would lose a settlement the worker was told
    /// succeeded. The replay's commit must be a real writer-queue barrier
    /// (`Pending`), which resolves once everything enqueued before it is
    /// durable; an in-memory journal (nothing to fsync) stays `Ready`.
    #[test]
    fn an_eventless_replay_commit_is_a_real_durability_barrier() {
        let dir = std::env::temp_dir().join(format!("nanobpmn-barrier-{}", std::process::id()));
        let path = dir.join("test.journal");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let mut journal = Journal::open(&path).unwrap();
        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let _ = journal
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        let activation = journal
            .apply_command_at(
                Command::activate_jobs_with_options(
                    "demo-work",
                    "w1",
                    1,
                    60_000,
                    0,
                    nanobpmn_engine_core::JobActivationOptions {
                        with_lease: true,
                        ..Default::default()
                    },
                ),
                0,
            )
            .unwrap()
            .0;
        let (job_key, token) = activation
            .iter()
            .find_map(|e| match e {
                Event::JobActivated {
                    job_key,
                    lease_token: Some(t),
                    ..
                } => Some((*job_key, t.clone())),
                _ => None,
            })
            .unwrap();

        let settle = Command::complete_job(job_key).with_lease_token(Some(token));
        let (events, original) = journal.apply_command_at(settle.clone(), 1).unwrap();
        assert!(!events.is_empty(), "the first settlement really settles");
        assert!(
            matches!(original, Commit(CommitInner::Pending(_))),
            "a real settlement on a file-backed journal is a pending write"
        );
        let replay = journal.apply_command_at(settle, 2).unwrap();
        assert!(
            replay.0.is_empty(),
            "the replay is an eventless idempotent success"
        );
        assert!(
            matches!(replay.1, Commit(CommitInner::Pending(_))),
            "the replay's ack rides the writer queue behind the original, never a bare ready commit"
        );
        // An unrelated eventless no-op (not a settlement) keeps a ready commit:
        // no barrier, no fsync, no commit statistics.
        let noop = journal
            .apply_command_at(Command::ExpireJobs { now: 3 }, 3)
            .unwrap();
        assert!(noop.0.is_empty());
        assert!(
            matches!(noop.1, Commit(CommitInner::Ready)),
            "a non-settlement no-op must not take a writer barrier"
        );
        // Both resolve once the writer drains (no deadlock).
        block_on(original.wait());
        block_on(replay.1.wait());

        // The in-memory journal has no writer: its barrier is already durable.
        let mut memory = Journal::in_memory();
        let _ = memory
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let _ = memory
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        let activation = memory
            .apply_command_at(
                Command::activate_jobs_with_options(
                    "demo-work",
                    "w1",
                    1,
                    60_000,
                    0,
                    nanobpmn_engine_core::JobActivationOptions {
                        with_lease: true,
                        ..Default::default()
                    },
                ),
                0,
            )
            .unwrap()
            .0;
        let (job_key, token) = activation
            .iter()
            .find_map(|e| match e {
                Event::JobActivated {
                    job_key,
                    lease_token: Some(t),
                    ..
                } => Some((*job_key, t.clone())),
                _ => None,
            })
            .unwrap();
        let settle = Command::complete_job(job_key).with_lease_token(Some(token));
        let _ = memory.apply_command_at(settle.clone(), 1).unwrap();
        let replay = memory.apply_command_at(settle, 2).unwrap();
        assert!(replay.0.is_empty());
        assert!(
            matches!(replay.1, Commit(CommitInner::Ready)),
            "an in-memory journal is already durable"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reopening_a_journal_replays_persisted_state() {
        // given a journal file in a temp dir with a deploy + an instance
        let dir = std::env::temp_dir().join(format!("nanobpmn-journal-{}", std::process::id()));
        let path = dir.join("test.journal");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();

        let instance_key = {
            let mut journal = Journal::open(&path).unwrap();
            assert!(journal.is_fresh());
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            events.iter().find_map(|e| e.instance_key()).unwrap()
        };

        // when a fresh journal is opened over the same file
        let reopened = Journal::open(&path).unwrap();

        // then it is not fresh and the instance is recovered
        assert!(!reopened.is_fresh());
        assert!(reopened.instance(instance_key).is_some());
        // and the demo process is deployed exactly once (still version 1)
        assert_eq!(reopened.state().processes.len(), 1);

        let _ = std::fs::remove_dir_all(&dir);
    }

    fn cold_journal() -> Journal {
        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Watermarks are irrelevant: tests drive spill via force_cold_spill_all.
        journal.set_cold_spill(store, 1, 0);
        journal
    }

    fn deploy_and_create(journal: &mut Journal) -> Key {
        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let (events, _) = journal
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        events.iter().find_map(|e| e.instance_key()).unwrap()
    }

    #[test]
    fn cold_spilled_instance_rehydrates_on_job_activation_and_completes() {
        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);

        // the instance is parked on the "demo-work" job: dormant and spillable
        assert!(journal.instance(key).is_some());
        assert_eq!(journal.force_cold_spill_all(), 1);
        assert_eq!(journal.cold_count(), 1);
        // its whole control state is now off-heap
        assert!(journal.instance(key).is_none());

        // a worker poll rehydrates it from the cold store and hands back the job
        let jobs = journal.activate_jobs("demo-work", "w", 1, 1_000, 0);
        assert_eq!(jobs.len(), 1);
        assert_eq!(jobs[0].instance_key, key);
        assert_eq!(journal.cold_count(), 0);
        assert!(journal.instance(key).is_some());

        // completing the rehydrated job drives the instance to completion
        let (events, _) = journal
            .apply_command(Command::complete_job(jobs[0].key))
            .unwrap();
        assert!(events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceCompleted { instance_key } if *instance_key == key)));
    }

    // Copilot on #1418: modification/migration/cancel of a parent must page in
    // its cold-spilled call-activity children, or tearing down the call
    // activity misses the (non-resident) child, which later resumes orphaned.
    #[test]
    fn terminating_a_call_activity_rehydrates_and_terminates_its_cold_child() {
        let mut journal = cold_journal();
        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let parent = ProcessBuilder::new("caller")
            .start_event("s")
            .call_activity("call", "demo")
            .end_event("e")
            .connect("s", "call")
            .connect("call", "e")
            .build()
            .unwrap();
        let _ = journal
            .apply_command(Command::DeployProcess(parent))
            .unwrap();
        let (events, _) = journal
            .apply_command(Command::create_instance("caller"))
            .unwrap();
        let parent_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        let child_key = journal
            .state()
            .instances
            .values()
            .find(|i| i.parent_process_instance_key == Some(parent_key))
            .map(|i| i.key)
            .expect("child instance");
        assert_eq!(
            journal.force_cold_spill_all(),
            2,
            "parent and child spilled"
        );

        let (events, _) = journal
            .apply_command(Command::ModifyInstance {
                instance_key: parent_key,
                activate_instructions: vec![],
                terminate_instructions: vec![],
                terminate_element_ids: vec!["call".to_string()],
                move_instructions: vec![],
            })
            .unwrap();
        assert!(
            events.iter().any(|e| matches!(
                e,
                Event::ProcessInstanceTerminated { instance_key } if *instance_key == child_key
            )),
            "cold child is terminated with its call activity"
        );
        assert_eq!(journal.cold_count(), 0, "no cold child left behind");
        // Nothing for a worker to resume.
        assert!(
            journal
                .activate_jobs("demo-work", "w", 1, 1_000, 0)
                .is_empty()
        );
    }

    // Copilot on #1418: the cold-descendant traversal must not re-scan the whole
    // resident instance set per visited descendant (O(N × D) on the partition
    // command path). The resident parent→children index is built once, so the
    // traversal performs zero resident scans — while still walking through
    // resident intermediates to reach a cold grandchild.
    #[test]
    fn cold_descendant_traversal_indexes_resident_children_once() {
        let mut journal = cold_journal();
        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        for (id, called) in [("top", "mid"), ("mid", "leaf"), ("leaf", "demo")] {
            let process = ProcessBuilder::new(id)
                .start_event("s")
                .call_activity("call", called)
                .end_event("e")
                .connect("s", "call")
                .connect("call", "e")
                .build()
                .unwrap();
            let _ = journal
                .apply_command(Command::DeployProcess(process))
                .unwrap();
        }
        let (events, _) = journal
            .apply_command(Command::create_instance("top"))
            .unwrap();
        let top_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        let mid_key = journal
            .state()
            .instances
            .values()
            .find(|i| i.parent_process_instance_key == Some(top_key))
            .map(|i| i.key)
            .expect("mid instance");
        let leaf_key = journal
            .state()
            .instances
            .values()
            .find(|i| i.parent_process_instance_key == Some(mid_key))
            .map(|i| i.key)
            .expect("leaf instance");
        // Park the leaf on its demo-work job (activated but never completed), so
        // it stays active and spillable. Spill only it: the mid instance stays
        // resident, so the traversal must reach the cold leaf THROUGH a resident
        // intermediate.
        let leaf_jobs = journal.activate_jobs("demo-work", "w", 1, 1_000, 0);
        assert_eq!(leaf_jobs.len(), 1, "leaf parked on its job");
        let leaf_snapshot = journal
            .engine
            .snapshot_instance(leaf_key)
            .expect("leaf is active and spillable");
        let cold = journal.cold.as_mut().expect("cold spill configured");
        cold.store
            .put_cold(leaf_key, &leaf_snapshot)
            .expect("cold row written");
        cold.index.insert(&leaf_snapshot);
        assert!(journal.instance(leaf_key).is_none(), "leaf is cold");
        assert!(journal.instance(mid_key).is_some(), "mid stays resident");

        let _ = journal
            .apply_command(Command::ModifyInstance {
                instance_key: top_key,
                activate_instructions: vec![],
                terminate_instructions: vec![],
                terminate_element_ids: vec!["call".to_string()],
                move_instructions: vec![],
            })
            .unwrap();

        assert_eq!(
            journal.resident_child_scans, 0,
            "descendant traversal uses the resident index built once, not a \
             per-descendant scan of every resident instance"
        );
        assert_eq!(
            journal.cold_count(),
            0,
            "the cold leaf was rehydrated through its resident parent"
        );
    }

    #[test]
    fn recover_lease_rehydrates_a_cold_spilled_job_and_keeps_its_worker() {
        // #963: a promoted follower may hold the digest's job only in its cold
        // index; recovery must page it in, not silently miss the lease/worker.
        let mut journal = cold_journal();
        deploy_and_create(&mut journal);
        let job_key = *journal.state().jobs.keys().next().expect("a job");
        assert_eq!(journal.force_cold_spill_all(), 1);
        assert!(!journal.state().jobs.contains_key(&job_key));
        assert!(journal.recover_lease(job_key, 10_000, Some("w-1"), 0));
        assert_eq!(journal.cold_count(), 0);
        assert_eq!(
            journal.activated_lease_workers(),
            vec![(job_key, "w-1".to_string())]
        );
    }

    #[test]
    fn recover_lease_does_not_rehydrate_an_expired_cold_lease() {
        // #963: an expired digest entry must be rejected without paging the
        // cold instance in (else every digest tick churns it through disk).
        let mut journal = cold_journal();
        deploy_and_create(&mut journal);
        let job_key = *journal.state().jobs.keys().next().expect("a job");
        assert_eq!(journal.force_cold_spill_all(), 1);
        assert!(!journal.recover_lease(job_key, 100, Some("w-1"), 100));
        assert_eq!(journal.cold_count(), 1);
        assert!(!journal.state().jobs.contains_key(&job_key));
    }

    #[test]
    fn durable_activation_rehydrates_cold_jobs() {
        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);
        assert_eq!(journal.force_cold_spill_all(), 1);
        let (events, _) = journal
            .apply_command_at(
                Command::activate_jobs_with_options(
                    "demo-work",
                    "leased",
                    1,
                    1000,
                    0,
                    nanobpmn_engine_core::JobActivationOptions {
                        with_lease: true,
                        ..Default::default()
                    },
                ),
                0,
            )
            .unwrap();
        assert!(events.iter().any(|event| matches!(event,
            Event::JobActivated { instance_key, lease_token: Some(_), .. } if *instance_key == key)));
    }

    #[test]
    fn set_variables_on_an_element_scope_rehydrates_a_cold_instance() {
        use nanobpmn_engine_core::Value;

        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);

        // the active token scope at the service task — a valid SetVariables target
        // that is NOT the instance root key
        let scope_key = *journal
            .instance(key)
            .unwrap()
            .active
            .keys()
            .find(|k| **k != key)
            .expect("an element-instance scope");

        assert_eq!(journal.force_cold_spill_all(), 1);
        assert!(journal.instance(key).is_none());

        // SetVariables addresses the element scope: ensure_resident_for_command
        // must route it back to the cold instance and page it in (else the engine
        // would raise ScopeNotFound)
        let mut vars = std::collections::HashMap::new();
        vars.insert("approved".to_string(), Value::Bool(true));
        let (events, _) = journal
            .apply_command(Command::set_variables(scope_key, vars))
            .unwrap();
        assert_eq!(journal.cold_count(), 0);
        assert!(journal.instance(key).is_some());
        assert!(events.iter().any(
            |e| matches!(e, Event::VariablesUpdated { instance_key, .. } if *instance_key == key)
        ));
    }

    #[test]
    fn command_targeting_a_cold_instance_rehydrates_it_first() {
        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);

        assert_eq!(journal.force_cold_spill_all(), 1);
        assert!(journal.instance(key).is_none());

        // CancelInstance targets a cold instance by key: ensure_resident_for_command
        // must page it back in before the engine applies the command
        let (events, _) = journal
            .apply_command(Command::cancel_instance(key))
            .unwrap();
        assert_eq!(journal.cold_count(), 0);
        assert!(events
            .iter()
            .any(|e| matches!(e, Event::ProcessInstanceTerminated { instance_key } if *instance_key == key)));
    }

    #[test]
    fn retire_below_forgets_cold_rows_of_instances_under_the_mark() {
        // Regression for unbounded var-spill growth: a follower cold-spills a
        // dormant replica instance, then the leader's per-key retirement digest for
        // it is DROPPED (best-effort). The low-water backstop must still reclaim it
        // — from BOTH the cold index and the disk store — or its `cold` row leaks
        // forever (the file plateaus at the high-water mark, never reclaims).
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        let mut journal = Journal::in_memory();
        journal.set_cold_spill(Arc::clone(&store), 1, 0);
        let key = deploy_and_create(&mut journal);

        // The instance is dormant on its job: spill it to the cold tier.
        assert_eq!(journal.force_cold_spill_all(), 1);
        assert_eq!(journal.cold_count(), 1);
        assert!(journal.instance(key).is_none());

        // The digest never arrives; only the low-water mark (above `key`) does.
        // The backstop reaps it: index cleared AND the disk row forgotten.
        let reaped = journal.retire_below(key + 1, 100);
        assert_eq!(reaped, 1, "the cold instance below the mark is reaped");
        assert_eq!(journal.cold_count(), 0, "cold index entry dropped");
        assert!(
            store.try_get_cold(key).unwrap().is_none(),
            "cold disk row forgotten (no leak)"
        );
    }

    #[test]
    fn retire_instances_forgets_cold_index_entry_on_the_digest_path() {
        // The digest path forgets the disk row; it must also drop the in-RAM cold
        // index entry, or a cold-spilled replica instance named in a digest leaves
        // a stale routing entry (a slow RAM leak) behind.
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        let mut journal = Journal::in_memory();
        journal.set_cold_spill(Arc::clone(&store), 1, 0);
        let key = deploy_and_create(&mut journal);
        assert_eq!(journal.force_cold_spill_all(), 1);
        assert_eq!(journal.cold_count(), 1);

        journal.retire_instances(&[key]);
        assert_eq!(journal.cold_count(), 0, "cold index entry dropped");
        assert!(
            store.try_get_cold(key).unwrap().is_none(),
            "cold disk row forgotten"
        );
    }

    #[test]
    fn collect_live_instance_keys_excludes_resident_terminal_instances() {
        // Regression for the post-bounce "stuck active instances" symptom: a leader
        // (or a node that reclaimed a partition) keeps a completed instance's shell
        // resident until its exporter drives eviction. The read-model orphan
        // reconciler compares its `Active` rows against this live set, so a
        // resident-but-terminal shell must NOT appear here — otherwise an orphaned
        // `Active` read row whose engine instance is already `Completed` would be
        // treated as still live and never retired, pinning the active-backlog gauge.
        let mut journal = Journal::in_memory();
        let completed_key = deploy_and_create(&mut journal);

        // Drive the instance to Completed WITHOUT evicting it (no exporter runs in
        // this unit): activate then complete its only job.
        let jobs = journal.activate_jobs("demo-work", "w", 1, 1_000, 0);
        assert_eq!(jobs.len(), 1);
        let (events, _) = journal
            .apply_command(Command::complete_job(jobs[0].key))
            .unwrap();
        assert!(events.iter().any(|e| matches!(
            e,
            Event::ProcessInstanceCompleted { instance_key } if *instance_key == completed_key
        )));
        // The completed instance is still resident (nothing evicted it)...
        assert!(matches!(
            journal.instance(completed_key).map(|i| i.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Completed)
        ));

        // ...but it is terminal, so it is NOT a live instance.
        let mut live = std::collections::HashSet::new();
        journal.collect_live_instance_keys(&mut live);
        assert!(
            !live.contains(&completed_key),
            "a resident-but-Completed instance must not count as live"
        );

        // Sanity: a genuinely Active instance IS collected, and the resident
        // Completed one stays excluded alongside it.
        let (events, _) = journal
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        let active_key = events.iter().find_map(|e| e.instance_key()).unwrap();
        let mut live2 = std::collections::HashSet::new();
        journal.collect_live_instance_keys(&mut live2);
        assert!(
            live2.contains(&active_key),
            "an Active instance must count as live"
        );
        assert!(
            !live2.contains(&completed_key),
            "the resident Completed instance stays excluded"
        );
    }

    /// #1338 (rework of the reverted #1467): a crash between writing a cold row
    /// and the instance's terminal eviction forget — or a row written by an
    /// engine predating self-contained snapshots, whose routing index was never
    /// persisted — leaves a row no live state references. Rows are deliberately
    /// not wiped at boot (#1331) and the in-RAM cold index is not rebuilt at
    /// boot, so nothing will ever `forget` it: an orphan. The post-recovery GC
    /// must reap it while preserving a row that still belongs to a live
    /// instance.
    #[test]
    fn post_recovery_gc_reaps_cold_rows_orphaned_by_a_crash_before_completion() {
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));

        // Two parked instances cold-spilled to the store.
        let mut journal = Journal::in_memory();
        journal.set_cold_spill(Arc::clone(&store), 1, 0);
        let orphan_key = deploy_and_create(&mut journal);
        let live_key = deploy_and_create(&mut journal);
        assert_eq!(journal.force_cold_spill_all(), 2);
        assert!(store.try_get_cold(orphan_key).unwrap().is_some());
        assert!(store.try_get_cold(live_key).unwrap().is_some());

        // --- the crash window: `orphan_key` rehydrates and completes (its
        // terminal event is journaled), but the node dies BEFORE the eviction
        // forget reaches the store. Capture BOTH cold rows BEFORE the
        // rehydration forget drops them, so we can restore exactly the rows a
        // real crash leaves behind.
        let orphan_row = store
            .try_get_cold(orphan_key)
            .unwrap()
            .expect("orphan cold row present before rehydration");
        let live_row = store
            .try_get_cold(live_key)
            .unwrap()
            .expect("live cold row present before rehydration");
        let jobs = journal.activate_jobs("demo-work", "w", 2, 1_000, 0);
        let orphan_job = jobs
            .iter()
            .find(|j| j.instance_key == orphan_key)
            .expect("orphan job activated")
            .key;
        let _ = journal
            .apply_command(Command::complete_job(orphan_job))
            .unwrap();
        // Crash here: `evict_instances(&[orphan_key])` never runs, so its store
        // forget is lost; `live_key`'s row was dropped by its own rehydration
        // forget but its instance is still live. Re-write both rows the crash
        // left behind.
        store.put_cold(orphan_key, &orphan_row).unwrap();
        store.put_cold(live_key, &live_row).unwrap();
        assert!(store.try_get_cold(orphan_key).unwrap().is_some());
        assert!(store.try_get_cold(live_key).unwrap().is_some());

        // --- the restart: recover the pre-crash engine state into a FRESH
        // journal through the real recovery path (snapshot restore, exactly as
        // `bootstrap_member` / owned-partition boot do) — never by hand-seeding
        // a live set. The recovered engine holds `live_key` (parked, never
        // completed) and no trace of `orphan_key` (its completion was journaled
        // and folded into the snapshot); the in-RAM cold index is empty (never
        // rebuilt at boot). If recovery loses the live instance from the sets
        // supplied to GC, the sweep deletes its row — this test fails, as it
        // must: that is the production data-loss defect it guards.
        let snapshot = journal.engine_snapshot();
        let mut recovered = Journal::in_memory();
        recovered.set_cold_spill(Arc::clone(&store), 1, 0);
        recovered.restore_engine_from_snapshot(snapshot);
        assert!(
            recovered.instance(live_key).is_some(),
            "recovery restored the live instance"
        );
        assert!(
            recovered.instance(orphan_key).is_none(),
            "recovery carries no trace of the completed orphan"
        );
        assert_eq!(
            recovered.cold_count(),
            0,
            "cold index is not rebuilt at boot"
        );

        // The real GC entry point, driven by the recovered engine's own live
        // set — no augmentation.
        let collected = recovered.gc_offheap_orphans();
        assert!(
            store.try_get_cold(orphan_key).unwrap().is_none(),
            "orphaned cold row is reaped"
        );
        assert!(
            store.try_get_cold(live_key).unwrap().is_some(),
            "the live instance's cold row is preserved"
        );
        assert_eq!(collected, 1, "exactly the orphan row is collected");
        assert!(
            recovered.instance(live_key).is_some(),
            "the surviving instance is still resolvable after the sweep"
        );
    }

    /// #1338: a spill (variable payload) row whose instance reached a terminal
    /// state but whose eviction forget was lost to a crash is an orphan and must
    /// be reaped. A spill row whose instance is still live AND still carries the
    /// `variables_spilled` flag — a legacy snapshot can leave that flag pointing
    /// at the row, the only surviving copy of its variables — must be preserved.
    #[test]
    fn post_recovery_gc_reaps_orphaned_spill_rows_but_preserves_legacy_spilled_flags() {
        use nanobpmn_engine_core::Value;

        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        let mut journal = Journal::in_memory();
        // Budget 0: any variable-carrying instance spills immediately.
        journal.set_spill(Arc::clone(&store), 0);
        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();

        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Str("payload".to_string()));

        // Two instances, both parked with non-empty variables so both spill.
        let mut keys = Vec::new();
        for _ in 0..2 {
            let (events, _) = journal
                .apply_command(Command::create_instance_with("demo", vars.clone()))
                .unwrap();
            keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
        }
        let (orphan_key, legacy_key) = (keys[0], keys[1]);
        journal.force_spill_scan();
        assert!(journal.engine.is_variables_spilled(orphan_key));
        assert!(journal.engine.is_variables_spilled(legacy_key));
        assert!(store.try_get(orphan_key).unwrap().is_some());
        assert!(store.try_get(legacy_key).unwrap().is_some());

        // Drive `orphan_key` to a terminal state WITHOUT its eviction forget
        // (the crash window). Capture its spill payload BEFORE the activation
        // rehydration forget drops it, so we can restore exactly the row a real
        // crash leaves behind.
        let orphan_vars = store
            .try_get(orphan_key)
            .unwrap()
            .expect("orphan spill row present before activation");
        // Activate ONLY the orphan's job (max_jobs 1), so `legacy_key` stays
        // parked and still spilled — its row remains referenced by the flag.
        let jobs = journal.activate_jobs("demo-work", "w", 1, 1_000, 0);
        assert_eq!(jobs.len(), 1);
        assert_eq!(
            jobs[0].instance_key, orphan_key,
            "the oldest spilled instance activates first"
        );
        let orphan_job = jobs[0].key;
        let _ = journal
            .apply_command(Command::complete_job(orphan_job))
            .unwrap();
        // Completing rehydrates the orphan's variables (clearing its spilled
        // flag) but the crash loses the eviction forget: re-write the surviving
        // spill row. `legacy_key` stays parked and still spilled (referenced).
        store.put(orphan_key, &orphan_vars).unwrap();
        assert!(store.try_get(orphan_key).unwrap().is_some());

        // The GC reaps the terminal instance's orphaned spill row and preserves
        // the legacy spilled-flag row.
        let collected = journal.gc_offheap_orphans();
        assert!(
            store.try_get(orphan_key).unwrap().is_none(),
            "terminal instance's orphaned spill row is reaped"
        );
        assert!(
            store.try_get(legacy_key).unwrap().is_some(),
            "legacy spilled-flag row is preserved (still referenced)"
        );
        assert_eq!(collected, 1, "exactly the orphan row is collected");
    }

    /// #1467 finding (reworked in #1338): a resident **Suspended** instance
    /// still carries `variables_spilled` — suspending never rehydrates the
    /// payload — so its spill row is the only surviving copy of its variables.
    /// The GC must treat it as referenced, or resuming the instance after the
    /// sweep finds no variables (data loss).
    #[test]
    fn post_recovery_gc_preserves_a_suspended_instances_spill_row() {
        use nanobpmn_engine_core::Value;

        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        let mut journal = Journal::in_memory();
        journal.set_spill(Arc::clone(&store), 0);
        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();

        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Str("payload".to_string()));
        let (events, _) = journal
            .apply_command(Command::create_instance_with("demo", vars))
            .unwrap();
        let key = events.iter().find_map(|e| e.instance_key()).unwrap();
        journal.force_spill_scan();
        assert!(journal.engine.is_variables_spilled(key));
        assert!(store.try_get(key).unwrap().is_some());

        // Suspend the parked instance: it stays nonterminal and still spilled.
        let _ = journal
            .apply_command(Command::suspend_instance(key))
            .unwrap();
        assert!(matches!(
            journal.instance(key).map(|i| i.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Suspended)
        ));
        assert!(journal.engine.is_variables_spilled(key));

        let collected = journal.gc_offheap_orphans();
        assert!(
            store.try_get(key).unwrap().is_some(),
            "a Suspended instance's still-referenced spill row must survive the sweep"
        );
        assert_eq!(collected, 0, "nothing is orphaned here");
    }

    /// #1467 finding (reworked in #1338): a resident **Terminating** instance —
    /// a cancel deferred on a user task's `canceling` listener chain (ADR 0037
    /// §6) — is nonterminal and can still carry `variables_spilled`. Its spill
    /// row must survive the sweep: the canceling listeners (and the terminal
    /// eviction that follows them) still need the payload.
    #[test]
    fn post_recovery_gc_preserves_a_terminating_instances_spill_row() {
        use nanobpmn_engine_core::{ProcessBuilder, TaskListener, TaskListenerEventType, Value};

        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        let mut journal = Journal::in_memory();
        journal.set_spill(Arc::clone(&store), 0);
        // A parallel split: one branch parks on a spillable service-task job,
        // the other on a user task carrying a `canceling` listener. Cancelling
        // then defers on the listener chain, parking the instance in
        // Terminating while its root variables stay spilled.
        let def = ProcessBuilder::new("approval")
            .start_event("start")
            .parallel_gateway("fork")
            .service_task("work", "demo-work")
            .user_task("review")
            .parallel_gateway("join")
            .end_event("end")
            .connect("start", "fork")
            .connect("fork", "work")
            .connect("fork", "review")
            .connect("work", "join")
            .connect("review", "join")
            .connect("join", "end")
            .with_task_listeners(
                "review",
                vec![TaskListener {
                    event_type: TaskListenerEventType::Canceling,
                    job_type: "onCancel".to_string(),
                    retries: None,
                }],
            )
            .build()
            .expect("valid listener process");
        let _ = journal.apply_command(Command::DeployProcess(def)).unwrap();

        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Str("payload".to_string()));
        let (events, _) = journal
            .apply_command(Command::create_instance_with("approval", vars))
            .unwrap();
        let key = events.iter().find_map(|e| e.instance_key()).unwrap();
        journal.force_spill_scan();
        assert!(
            journal.engine.is_variables_spilled(key),
            "the instance spilled while Active on its service-task job"
        );
        assert!(store.try_get(key).unwrap().is_some());

        // Cancel: the `canceling` listener defers termination, so the instance
        // parks in Terminating — still nonterminal, still spilled (cancel never
        // rehydrates a spilled payload).
        let _ = journal
            .apply_command(Command::cancel_instance(key))
            .unwrap();
        assert!(matches!(
            journal.instance(key).map(|i| i.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Terminating)
        ));
        assert!(
            journal.engine.is_variables_spilled(key),
            "cancel does not rehydrate a spilled payload"
        );

        let collected = journal.gc_offheap_orphans();
        assert!(
            store.try_get(key).unwrap().is_some(),
            "a Terminating instance's still-referenced spill row must survive the sweep"
        );
        assert_eq!(collected, 0, "nothing is orphaned here");
    }

    /// #1467 finding (reworked in #1338): the live set the GC diffs the store
    /// against must include EVERY nonterminal instance — Active, Suspended and
    /// Terminating — not just Active. A Suspended or Terminating instance keeps
    /// its runtime state (and can retain `variables_spilled`), so excluding it
    /// reaps rows that are still referenced.
    #[test]
    fn collect_live_instance_keys_includes_every_nonterminal_state() {
        use nanobpmn_engine_core::ProcessInstanceState;

        let mut journal = Journal::in_memory();
        let suspended_key = deploy_and_create(&mut journal);
        let active_key = deploy_and_create(&mut journal);
        let _ = journal
            .apply_command(Command::suspend_instance(suspended_key))
            .unwrap();
        assert!(matches!(
            journal.instance(suspended_key).map(|i| i.state),
            Some(ProcessInstanceState::Suspended)
        ));

        let mut live = std::collections::HashSet::new();
        journal.collect_live_instance_keys(&mut live);
        assert!(live.contains(&active_key), "an Active instance is live");
        assert!(
            live.contains(&suspended_key),
            "a Suspended instance is nonterminal and must count as live"
        );
    }

    #[test]
    fn evicting_a_terminal_instance_drops_its_spilled_variable_row() {
        use nanobpmn_engine_core::Value;

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Budget 0: any dormant, variable-carrying instance spills immediately.
        journal.set_spill(Arc::clone(&store), 0);

        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Str("payload".to_string()));

        // Two instances, both parked on the demo-work job with non-empty variables,
        // so maybe_spill sheds both their payloads to the store.
        let mut keys = Vec::new();
        for _ in 0..2 {
            let (events, _) = journal
                .apply_command(Command::create_instance_with("demo", vars.clone()))
                .unwrap();
            keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
        }
        let (a, b) = (keys[0], keys[1]);
        // The first over-budget command sheds eagerly; subsequent shedding is
        // amortised, so drive the scan deterministically to reach the fully-shed
        // state this test asserts on.
        journal.force_spill_scan();
        assert!(journal.engine.is_variables_spilled(a), "a spilled");
        assert!(journal.engine.is_variables_spilled(b), "b spilled");

        // Evicting the (now terminal) instance a must drop its spill row, while
        // leaving the still-live instance b's payload in the store.
        journal.evict_instances(&[a]);

        assert!(
            store.try_get(a).unwrap().is_none(),
            "evicted instance's spill row is gone"
        );
        assert!(
            store.try_get(b).unwrap().is_some(),
            "a live instance's spill row is untouched"
        );
    }

    #[test]
    fn adaptive_spill_keeps_everything_resident_below_pressure() {
        use nanobpmn_engine_core::Value;

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Adaptive with an unreachable high-water and the per-command backstop
        // disabled (hard_cap 0): below RAM pressure nothing should ever spill, so
        // the workload runs at full throughput with every payload resident.
        journal.set_var_spill_adaptive(Arc::clone(&store), u64::MAX, u64::MAX, u64::MAX, 0, 0);

        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Str("payload".to_string()));

        let mut keys = Vec::new();
        for _ in 0..3 {
            let (events, _) = journal
                .apply_command(Command::create_instance_with("demo", vars.clone()))
                .unwrap();
            keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
        }
        // Even a forced per-command scan is a no-op: the adaptive trigger only
        // sheds on the RSS sweep (or the hard-cap backstop, here disabled).
        journal.force_spill_scan();
        assert_eq!(
            journal.engine.resident_spillable_count(),
            3,
            "no instance spills below the memory watermark"
        );
        for k in &keys {
            assert!(!journal.engine.is_variables_spilled(*k));
        }
    }

    #[test]
    fn spilled_nested_scope_instance_activates_with_the_merged_view() {
        use nanobpmn_engine_core::{ProcessBuilder, Value};

        // A sub-process with an input mapping is its own variable scope: the
        // mapping creates a scope-local `scoped` (= seed + 1) that a job inside
        // the sub-process sees merged over the root `seed`.
        fn nested() -> nanobpmn_engine_core::ProcessDefinition {
            ProcessBuilder::new("nested")
                .start_event("start")
                .sub_process("sub", "sub_start")
                .with_io(
                    "sub",
                    nanobpmn_engine_core::IoMapping {
                        inputs: vec![nanobpmn_engine_core::Mapping {
                            source: "=seed + 1".to_string(),
                            target: "scoped".to_string(),
                        }],
                        outputs: Vec::new(),
                    },
                )
                .start_event("sub_start")
                .contained_in("sub_start", "sub")
                .service_task("inner", "inner-work")
                .contained_in("inner", "sub")
                .end_event("sub_end")
                .contained_in("sub_end", "sub")
                .end_event("done")
                .connect("start", "sub")
                .connect("sub_start", "inner")
                .connect("inner", "sub_end")
                .connect("sub", "done")
                .build()
                .expect("valid nested process")
        }

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Budget 0: the job-parked instance spills its root payload immediately.
        journal.set_spill(Arc::clone(&store), 0);

        let _ = journal
            .apply_command(Command::DeployProcess(nested()))
            .unwrap();
        let mut vars = std::collections::HashMap::new();
        vars.insert("seed".to_string(), Value::Int(4));
        let (events, _) = journal
            .apply_command(Command::create_instance_with("nested", vars))
            .unwrap();
        let key = events.iter().find_map(|e| e.instance_key()).unwrap();

        // Shed the root payload; the sub-process scope-local map stays resident.
        journal.force_spill_scan();
        assert!(
            journal.engine.is_variables_spilled(key),
            "the job-parked nested-scope instance spilled its root payload"
        );

        // Activating the inner job rehydrates the root AND recomputes the merged
        // scoped view: the worker must receive BOTH the restored root `seed` and
        // the resident sub-process-local `scoped` — not the root-only payload.
        let activated = journal.activate_jobs("inner-work", "w", 1, 60_000, 0);
        assert_eq!(activated.len(), 1, "the inner job activates");
        let job = &activated[0];
        assert_eq!(
            job.variables.get("seed"),
            Some(&Value::Int(4)),
            "root variable restored from the spill store"
        );
        assert_eq!(
            job.variables.get("scoped"),
            Some(&Value::Int(5)),
            "sub-process scope-local variable folded back into the worker's view"
        );
        assert!(!journal.engine.is_variables_spilled(key), "rehydrated");
    }

    #[test]
    fn migration_of_a_spilled_instance_arms_boundaries_with_restored_variables() {
        use nanobpmn_engine_core::{Event, ProcessBuilder, Value};

        // A job-parked instance whose root variables are spilled is migrated onto
        // a target that adds a message boundary correlated on `=key`. Migration
        // performs no job activation, so the journal must restore the spilled
        // root BEFORE planning, or the new subscription gets an empty key.
        fn def(id: &str, with_boundary: bool) -> nanobpmn_engine_core::ProcessDefinition {
            let mut b = ProcessBuilder::new(id)
                .start_event("start")
                .service_task("task", "work")
                .end_event("end")
                .connect("start", "task")
                .connect("task", "end");
            if with_boundary {
                b = b
                    .message_boundary_event("mb", "task", "cancel", "=key")
                    .end_event("cancelled")
                    .connect("mb", "cancelled");
            }
            b.build().expect("valid")
        }
        let deployed_key = |events: &[Event]| {
            events
                .iter()
                .find_map(|e| match e {
                    Event::ProcessDeployed {
                        process_definition_key,
                        ..
                    } => Some(*process_definition_key),
                    _ => None,
                })
                .expect("deployed")
        };

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        journal.set_spill(Arc::clone(&store), 0);
        let _ = journal
            .apply_command(Command::DeployProcess(def("source", false)))
            .unwrap();
        let (events, _) = journal
            .apply_command(Command::DeployProcess(def("target", true)))
            .unwrap();
        let target_key = deployed_key(&events);
        let mut vars = std::collections::HashMap::new();
        vars.insert("key".to_string(), Value::Str("k1".into()));
        let (events, _) = journal
            .apply_command(Command::create_instance_with("source", vars))
            .unwrap();
        let inst = events.iter().find_map(|e| e.instance_key()).unwrap();
        journal.force_spill_scan();
        assert!(journal.engine.is_variables_spilled(inst), "spilled");

        let (events, _) = journal
            .apply_command(Command::migrate_instance(
                inst,
                target_key,
                vec![("task".into(), "task".into())],
            ))
            .unwrap();
        let key = events
            .iter()
            .find_map(|e| match e {
                Event::MessageSubscriptionCreated {
                    correlation_key, ..
                } => Some(correlation_key.clone()),
                _ => None,
            })
            .expect("new boundary subscription opened");
        assert_eq!(key, "k1", "correlation key evaluated against restored root");
        assert!(!journal.engine.is_variables_spilled(inst), "rehydrated");
    }

    #[test]
    fn parallel_jobs_of_a_spilled_instance_all_carry_restored_variables() {
        use nanobpmn_engine_core::{ProcessBuilder, Value};

        // #1331: when one spilled instance has several jobs activated in the SAME
        // batch (a parallel gateway fan-out), the first restore clears
        // `variables_spilled`; the later jobs for that instance must STILL receive
        // the restored variables, not the empty placeholder projection they were
        // skipped into when the spilled branch short-circuited on the cleared flag.
        fn fork() -> nanobpmn_engine_core::ProcessDefinition {
            ProcessBuilder::new("fork")
                .start_event("start")
                .parallel_gateway("split")
                .service_task("a", "demo-work")
                .service_task("b", "demo-work")
                .parallel_gateway("join")
                .end_event("end")
                .connect("start", "split")
                .connect("split", "a")
                .connect("split", "b")
                .connect("a", "join")
                .connect("b", "join")
                .connect("join", "end")
                .build()
                .expect("valid fork process")
        }

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Budget 0: the job-parked instance sheds its root payload immediately.
        journal.set_spill(Arc::clone(&store), 0);

        let _ = journal
            .apply_command(Command::DeployProcess(fork()))
            .unwrap();
        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Int(7));
        let (events, _) = journal
            .apply_command(Command::create_instance_with("fork", vars))
            .unwrap();
        let key = events.iter().find_map(|e| e.instance_key()).unwrap();

        // Both parallel tasks are parked on `demo-work`; shed the root payload.
        journal.force_spill_scan();
        assert!(
            journal.engine.is_variables_spilled(key),
            "the job-parked fork instance spilled its root payload"
        );

        // ONE activation batch returns BOTH jobs of the same spilled instance.
        let activated = journal.activate_jobs("demo-work", "w", 2, 60_000, 0);
        assert_eq!(
            activated.len(),
            2,
            "both parallel jobs of the instance activate in one batch"
        );
        assert!(
            activated.iter().all(|j| j.instance_key == key),
            "both jobs belong to the spilled instance"
        );
        for job in &activated {
            assert_eq!(
                job.variables.get("data"),
                Some(&Value::Int(7)),
                "every parallel job must carry the restored root variable, not the \
                 empty placeholder projection"
            );
        }
    }

    #[test]
    fn snapshot_install_clears_stale_cold_routing_index() {
        // Raft-receiver regression (#1331): a self-contained snapshot folds every
        // cold (wholly off-heap) instance back in as resident, so a receiver's own
        // pre-existing cold routing index is entirely stale after the install. If
        // it is not cleared, a later command for a key this node happened to hold
        // cold routes through `rehydrate_cold` and clobbers the freshly installed
        // resident state with the superseded cold row (or resurrects an instance
        // the authoritative snapshot does not even contain).
        let mut receiver = cold_journal();
        let ghost = deploy_and_create(&mut receiver);
        assert_eq!(receiver.force_cold_spill_all(), 1);
        assert_eq!(receiver.cold_count(), 1);
        assert!(
            receiver.instance(ghost).is_none(),
            "the instance is off-heap"
        );

        // A self-contained snapshot from a peer (its own resident instance).
        let captured = {
            let mut src = Journal::in_memory();
            let _ = src.apply_command(Command::DeployProcess(demo())).unwrap();
            let _ = src.apply_command(Command::create_instance("demo")).unwrap();
            src.engine_snapshot()
        };

        receiver.restore_engine_from_snapshot(captured);

        assert!(!receiver.is_fresh());
        assert_eq!(
            receiver.cold_count(),
            0,
            "a self-contained snapshot install must clear the receiver's stale cold \
             routing index, so no later command can rehydrate_cold a superseded row \
             over the freshly installed resident state"
        );
    }

    #[test]
    fn adaptive_hard_cap_backstop_sheds_down_to_the_cap() {
        use nanobpmn_engine_core::Value;

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        // Unreachable high-water (so the RSS sweep never fires in the test), but a
        // hard_cap of 1: the per-command backstop must keep at most 1 resident
        // spill candidate even without RAM pressure — the inter-sweep OOM guard.
        journal.set_var_spill_adaptive(Arc::clone(&store), u64::MAX, u64::MAX, u64::MAX, 0, 1);

        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Str("payload".to_string()));

        for _ in 0..3 {
            let (_events, _commit) = journal
                .apply_command(Command::create_instance_with("demo", vars.clone()))
                .unwrap();
        }
        journal.force_spill_scan();
        assert_eq!(
            journal.engine.resident_spillable_count(),
            1,
            "the hard-cap backstop sheds the backlog down to hard_cap"
        );
    }

    #[test]
    fn restore_engine_from_snapshot_preserves_exporter_and_evicts_completions() {
        // Regression: a Raft snapshot install/restore must NOT drop the journal's
        // read-model exporter wiring. An owned partition reclaiming via a snapshot
        // install relies on the exporter (not self-eviction) to shed its completed
        // instances; if the exporter is torn down the completed backlog piles up
        // unbounded in hot state (the ~830k-per-partition leak on a returning
        // owner). Prove the exporter survives the install and still sees events.

        // A "received" snapshot from a peer: a deployed demo + one still-live
        // instance AND one already-completed instance (the exporter-lag / terminal
        // shell a leader carries at snapshot-build time — `Engine::snapshot` clones
        // the whole state including terminal instances).
        let (captured, completed_key) = {
            let mut src = Journal::in_memory_partition(0);
            let _ = src.apply_command(Command::DeployProcess(demo())).unwrap();
            // Instance #1: drive it to Completed and leave it resident.
            let (ev, _) = src.apply_command(Command::create_instance("demo")).unwrap();
            let done_key = ev.iter().find_map(|e| e.instance_key()).unwrap();
            let jobs = src.activate_jobs("demo-work", "w", 1, 1_000, 0);
            let _ = src
                .apply_command(Command::complete_job(jobs[0].key))
                .unwrap();
            assert!(matches!(
                src.instance(done_key).map(|i| i.state),
                Some(nanobpmn_engine_core::ProcessInstanceState::Completed)
            ));
            // Instance #2: parked on its job, still Active.
            let _ = src.apply_command(Command::create_instance("demo")).unwrap();
            (src.engine_snapshot(), done_key)
        };

        // The local owned journal: exporter wired at boot, then a snapshot lands.
        let (tx, rx) = mpsc::channel::<ExportBatch>();
        let mut journal = Journal::in_memory_partition(0);
        journal.set_exporter(tx, Arc::new(AtomicU64::new(0)));

        journal.restore_engine_from_snapshot(captured);

        // The snapshot's terminal shell is shed (it never flows through this node's
        // exporter, so it must not linger in hot state), while the live instance
        // stays resident.
        assert!(!journal.is_fresh());
        assert_eq!(journal.state().processes.len(), 1);
        assert!(
            journal.instance(completed_key).is_none(),
            "an installed snapshot's terminal instance must be evicted from hot state"
        );
        assert_eq!(
            journal.state().instances.len(),
            1,
            "only the in-flight instance remains resident after the install"
        );

        // The exporter is still wired: a subsequent command's events flow to the
        // read-model channel.
        let (events, _) = journal
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        let key = events.iter().find_map(|e| e.instance_key()).unwrap();
        let batch = rx
            .try_recv()
            .expect("exporter must still receive events after a snapshot install");
        assert!(
            batch.events.iter().any(|e| e.instance_key() == Some(key)),
            "the created instance's events must reach the preserved exporter"
        );
    }

    /// A unique on-disk spill-store path, so a test can close and **reopen** the
    /// store exactly as a process restart does (an in-memory store cannot model
    /// the boot path).
    fn restart_store_path(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-spill-restart-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("var-spill.sqlite")
    }

    /// Spills `n` demo instances' variables (`data = "payload-<i>"`) to a
    /// file-backed store at `path`, returning the journal and the instance keys.
    fn spilled_journal(path: &std::path::Path, n: usize) -> (Journal, Vec<Key>) {
        use nanobpmn_engine_core::Value;

        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(Some(path)).unwrap());
        journal.set_spill(store, 0);
        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let mut keys = Vec::new();
        for i in 0..n {
            let mut vars = std::collections::HashMap::new();
            vars.insert("data".to_string(), Value::Str(format!("payload-{i}")));
            let (events, _) = journal
                .apply_command(Command::create_instance_with("demo", vars))
                .unwrap();
            keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
        }
        journal.force_spill_scan();
        for key in &keys {
            assert!(
                journal.engine.is_variables_spilled(*key),
                "precondition: spilled"
            );
        }
        (journal, keys)
    }

    /// Boots a fresh journal over a **reopened** store at `path` and restores
    /// `snapshot` into it — the snapshot-recovery boot path (#1331).
    fn reboot_from(
        path: &std::path::Path,
        snapshot: nanobpmn_engine_core::EngineSnapshot,
    ) -> Journal {
        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(Some(path)).unwrap());
        journal.set_spill(store, usize::MAX);
        journal.restore_engine_from_snapshot(snapshot);
        journal
    }

    fn assert_jobs_carry_payloads(journal: &mut Journal, n: usize) {
        use nanobpmn_engine_core::Value;

        let activated = journal.activate_jobs("demo-work", "w", n, 60_000, 0);
        assert_eq!(activated.len(), n, "every recovered job activates");
        let mut seen: Vec<String> = activated
            .iter()
            .map(|job| match job.variables.get("data") {
                Some(Value::Str(s)) => s.clone(),
                other => panic!(
                    "job {} activated without its spilled variables (got {other:?}) — #1331",
                    job.key
                ),
            })
            .collect();
        seen.sort();
        let mut want: Vec<String> = (0..n).map(|i| format!("payload-{i}")).collect();
        want.sort();
        assert_eq!(seen, want);
    }

    // #1331: a classic (full-variable) snapshot cut while instances are spilled
    // must be self-contained — it carries the real payloads, not the empty
    // placeholders — so a restart recovers every variable.
    #[test]
    fn classic_snapshot_of_spilled_instances_survives_restart() {
        let path = restart_store_path("snapshot");
        let (journal, keys) = spilled_journal(&path, 3);

        let snapshot = journal.engine_snapshot();
        for key in &keys {
            let inst = snapshot
                .state
                .instances
                .get(key)
                .expect("instance in snapshot");
            assert!(
                !inst.variables_spilled && inst.variables.contains_key("data"),
                "snapshot must carry the spilled payload, not a placeholder"
            );
        }
        // Spilled-ness is a live-memory artefact: capturing a snapshot must not
        // rehydrate (or consume the row of) the live instance.
        for key in &keys {
            assert!(
                journal.engine.is_variables_spilled(*key),
                "live state untouched"
            );
        }
        drop(journal);

        let mut rebooted = reboot_from(&path, snapshot);
        assert_jobs_carry_payloads(&mut rebooted, 3);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1331: a snapshot written by an older engine (placeholders + spilled flag)
    // must still recover: booting must not destroy the spill rows that recovered
    // state references.
    #[test]
    fn legacy_placeholder_snapshot_recovers_from_spill_rows_after_restart() {
        let path = restart_store_path("legacy");
        let (journal, _keys) = spilled_journal(&path, 2);
        // The pre-fix capture: a raw state clone with placeholders.
        let legacy = journal.engine.snapshot();
        drop(journal);

        let mut rebooted = reboot_from(&path, legacy);
        assert_jobs_carry_payloads(&mut rebooted, 2);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1331: a cold-spilled (wholly off-heap) instance must be in the snapshot,
    // or it silently vanishes on restart.
    #[test]
    fn classic_snapshot_includes_cold_spilled_instances() {
        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);
        assert!(
            journal.force_cold_spill_all() >= 1,
            "precondition: went cold"
        );
        assert!(journal.instance(key).is_none(), "precondition: off-heap");

        let snapshot = journal.engine_snapshot();
        assert!(
            snapshot.state.instances.contains_key(&key),
            "cold instance must be folded into the snapshot"
        );
        assert!(
            snapshot.state.jobs.values().any(|j| j.instance_key == key),
            "its jobs too"
        );
        assert_eq!(journal.cold_count(), 1, "live cold state untouched");

        let mut rebooted = Journal::in_memory();
        rebooted.restore_engine_from_snapshot(snapshot);
        assert!(rebooted.instance(key).is_some(), "recovered after restart");
        assert_eq!(
            rebooted.activate_jobs("demo-work", "w", 1, 60_000, 0).len(),
            1
        );
    }

    // #1331 defect-class guard: spill is a live-memory artefact and must be
    // invisible to snapshots. Whatever mix of variable spill and cold spill is in
    // effect, `engine_snapshot` must capture the same instances, jobs and
    // variables as an identical journal that never spilled.
    #[test]
    fn snapshot_is_invariant_under_variable_and_cold_spill() {
        use nanobpmn_engine_core::Value;

        // Two batches of two instances; `between` runs after the first batch.
        fn drive(journal: &mut Journal, between: fn(&mut Journal)) -> Vec<Key> {
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let mut keys = Vec::new();
            for i in 0..4 {
                if i == 2 {
                    between(journal);
                }
                let mut vars = std::collections::HashMap::new();
                vars.insert("data".to_string(), Value::Str(format!("p{i}")));
                vars.insert("n".to_string(), Value::Int(i));
                let (events, _) = journal
                    .apply_command(Command::create_instance_with("demo", vars))
                    .unwrap();
                keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
            }
            keys
        }
        /// (instance key, sorted (name, value) variables, sorted job keys).
        type InstanceView = (Key, Vec<(String, String)>, Vec<Key>);
        fn view(snap: &nanobpmn_engine_core::EngineSnapshot) -> Vec<InstanceView> {
            let mut out: Vec<_> = snap
                .state
                .instances
                .values()
                .map(|inst| {
                    assert!(!inst.variables_spilled, "no placeholder in a snapshot");
                    let mut vars: Vec<(String, String)> = inst
                        .variables
                        .iter()
                        .map(|(k, v)| (k.clone(), format!("{v:?}")))
                        .collect();
                    vars.sort();
                    let mut jobs: Vec<Key> = snap
                        .state
                        .jobs
                        .values()
                        .filter(|j| j.instance_key == inst.key)
                        .map(|j| j.key)
                        .collect();
                    jobs.sort();
                    (inst.key, vars, jobs)
                })
                .collect();
            out.sort();
            out
        }

        let mut resident = Journal::in_memory();
        let keys = drive(&mut resident, |_| {});
        let expected = view(&resident.engine_snapshot());

        let mut spilled = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).unwrap());
        spilled.set_spill(Arc::clone(&store), usize::MAX);
        spilled.set_cold_spill(Arc::clone(&store), u64::MAX, u64::MAX);
        // First batch goes wholly cold; the second has its variables shed.
        let spilled_keys = drive(&mut spilled, |j| {
            assert_eq!(
                j.force_cold_spill_all(),
                2,
                "precondition: first batch cold"
            );
        });
        assert_eq!(spilled_keys, keys);
        spilled.set_spill(store, 0);
        spilled.force_spill_scan();
        assert_eq!(spilled.cold_count(), 2);
        for key in &keys[2..] {
            assert!(
                spilled.engine.is_variables_spilled(*key),
                "second batch spilled"
            );
        }

        assert_eq!(view(&spilled.engine_snapshot()), expected);
    }

    // #1337 defect-class guard: the streaming/sidecar capture must produce the
    // SAME logical snapshot as the folding capture (every instance, every
    // variable) — spill is invisible either way — while never folding the whole
    // cold set into one in-memory `EngineSnapshot`. This drives a journal with a
    // mix of cold-spilled and variable-spilled instances through
    // `write_streaming_snapshot_to`, restores it by streaming, and asserts the
    // rebuilt engine matches a never-spilled twin.
    #[test]
    fn streaming_snapshot_is_invariant_under_variable_and_cold_spill() {
        use nanobpmn_engine_core::Value;

        fn drive(journal: &mut Journal, between: fn(&mut Journal)) -> Vec<Key> {
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let mut keys = Vec::new();
            for i in 0..4 {
                if i == 2 {
                    between(journal);
                }
                let mut vars = std::collections::HashMap::new();
                vars.insert("data".to_string(), Value::Str(format!("p{i}")));
                vars.insert("n".to_string(), Value::Int(i));
                let (events, _) = journal
                    .apply_command(Command::create_instance_with("demo", vars))
                    .unwrap();
                keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
            }
            keys
        }
        type InstanceView = (Key, Vec<(String, String)>, Vec<Key>);
        fn view(snap: &nanobpmn_engine_core::EngineSnapshot) -> Vec<InstanceView> {
            let mut out: Vec<_> = snap
                .state
                .instances
                .values()
                .map(|inst| {
                    assert!(!inst.variables_spilled, "no placeholder in a snapshot");
                    let mut vars: Vec<(String, String)> = inst
                        .variables
                        .iter()
                        .map(|(k, v)| (k.clone(), format!("{v:?}")))
                        .collect();
                    vars.sort();
                    let mut jobs: Vec<Key> = snap
                        .state
                        .jobs
                        .values()
                        .filter(|j| j.instance_key == inst.key)
                        .map(|j| j.key)
                        .collect();
                    jobs.sort();
                    (inst.key, vars, jobs)
                })
                .collect();
            out.sort();
            out
        }

        let mut resident = Journal::in_memory();
        let keys = drive(&mut resident, |_| {});
        let expected = view(&resident.engine_snapshot());

        // The spilled twin: first batch wholly cold, second batch variable-spilled.
        let mut spilled = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).unwrap());
        spilled.set_spill(Arc::clone(&store), usize::MAX);
        spilled.set_cold_spill(Arc::clone(&store), u64::MAX, u64::MAX);
        let spilled_keys = drive(&mut spilled, |j| {
            assert_eq!(
                j.force_cold_spill_all(),
                2,
                "precondition: first batch cold"
            );
        });
        assert_eq!(spilled_keys, keys);
        spilled.set_spill(store, 0);
        spilled.force_spill_scan();
        assert_eq!(spilled.cold_count(), 2);
        for key in &keys[2..] {
            assert!(
                spilled.engine.is_variables_spilled(*key),
                "second batch spilled"
            );
        }

        // Capture via the streaming sidecar. The cold instances must NOT become
        // resident as a side effect (the whole point of #1337).
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-stream-snap-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("snapshot.stream.bin");
        spilled
            .write_streaming_snapshot_to(&path, "{\"format_version\":3}", 0)
            .expect("streaming capture succeeds");
        assert_eq!(
            spilled.cold_count(),
            2,
            "cold instances stayed off-heap through the streaming capture"
        );

        // Restore by streaming, rebuilding a full EngineSnapshot one instance at
        // a time, and confirm it is logically identical to the never-spilled twin.
        let mut control: Option<nanobpmn_engine_core::EngineSnapshot> = None;
        let mut restored = Vec::new();
        crate::snapstream::read_streaming_snapshot(
            &path,
            |c, _| control = Some(c),
            |snap| restored.push(snap),
        )
        .expect("streaming restore");
        let mut snap = control.expect("control frame present");
        for is in restored {
            nanobpmn_engine_core::restore_instance_snapshot(&mut snap.state, is);
        }
        assert_eq!(view(&snap), expected);

        let _ = std::fs::remove_dir_all(&dir);
    }

    // #1331 (lean path): a cold-spilled (wholly off-heap) instance must survive a
    // restart from a LEAN checkpoint too. Lean mode and cold spill are configured
    // independently, so they co-occur; `snapshot_control_only` omits the cold
    // instance and nothing rebuilds the cold table at boot, so without folding it
    // into the lean checkpoint it silently vanishes. The fold routes its control
    // state into the snapshot and its variables through the var-store delta.
    #[test]
    fn lean_checkpoint_includes_cold_spilled_instances() {
        use nanobpmn_engine_core::Value;

        let mut journal = Journal::in_memory();
        let varstore = Arc::new(crate::varstore::VarStore::open(None).unwrap());
        let cold_store = Arc::new(crate::varspill::VarSpillStore::open(None).unwrap());
        journal.set_varstore(Arc::clone(&varstore));
        journal.set_cold_spill(cold_store, 1, 0);

        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let mut vars = std::collections::HashMap::new();
        vars.insert("data".to_string(), Value::Str("cold-payload".to_string()));
        let (events, _) = journal
            .apply_command(Command::create_instance_with("demo", vars))
            .unwrap();
        let key = events.iter().find_map(|e| e.instance_key()).unwrap();

        // Shed the whole instance to the cold tier.
        assert!(
            journal.force_cold_spill_all() >= 1,
            "precondition: went cold"
        );
        assert!(journal.instance(key).is_none(), "precondition: off-heap");
        assert_eq!(journal.cold_count(), 1);

        // Cut a lean checkpoint: the cold instance's CONTROL state lands in the
        // snapshot (variable-free), its variables in the upsert delta.
        let (snap, cold_upserts) = journal
            .lean_snapshot_and_cold_upserts()
            .expect("cold fold succeeds");
        let inst = snap
            .state
            .instances
            .get(&key)
            .expect("cold instance folded into the lean snapshot");
        assert!(
            inst.variables.is_empty(),
            "lean snapshot stays variable-free"
        );
        assert!(
            snap.state.jobs.values().any(|j| j.instance_key == key),
            "its jobs are folded in too"
        );
        let folded_vars = cold_upserts
            .iter()
            .find(|(k, _)| *k == key)
            .map(|(_, v)| Arc::clone(v))
            .expect("cold instance variables routed through the upsert delta");
        assert_eq!(
            folded_vars.get("data"),
            Some(&Value::Str("cold-payload".to_string()))
        );

        // Persist the delta to the authoritative store (as the checkpoint loop
        // does), then recover: from_snapshot + install_variables(store).
        let ups: Vec<(Key, &HashMap<String, Value>)> =
            cold_upserts.iter().map(|(k, v)| (*k, v.as_ref())).collect();
        varstore.checkpoint(0, 1, &ups, &[]).unwrap();

        let mut rebooted = Journal::in_memory();
        rebooted.set_varstore(Arc::clone(&varstore));
        rebooted.restore_engine_from_snapshot(snap);
        if let Some(stored) = varstore.try_get(key).unwrap() {
            rebooted.engine.install_variables(key, stored);
        }

        assert!(
            rebooted.instance(key).is_some(),
            "cold instance recovered after a lean restart"
        );
        let activated = rebooted.activate_jobs("demo-work", "w", 1, 60_000, 0);
        assert_eq!(activated.len(), 1, "recovered job activates");
        assert_eq!(
            activated[0].variables.get("data"),
            Some(&Value::Str("cold-payload".to_string())),
            "job activates with its variables — #1331 lean path"
        );
    }

    // ---- #1388: rehydration must never destroy an unreadable row nor drop an
    // instance / hand out a job with empty variables. ----

    fn raw_row_count(path: &std::path::Path, table: &str, key: Key) -> i64 {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.query_row(
            &format!("SELECT COUNT(*) FROM {table} WHERE key = ?1"),
            [key as i64],
            |r| r.get(0),
        )
        .unwrap()
    }

    fn corrupt_row(path: &std::path::Path, table: &str, col: &str, key: Key) {
        let conn = rusqlite::Connection::open(path).unwrap();
        let n = conn
            .execute(
                &format!("UPDATE {table} SET {col} = 'not-json{{' WHERE key = ?1"),
                [key as i64],
            )
            .unwrap();
        assert_eq!(n, 1, "precondition: row to corrupt exists");
    }

    /// A journal whose single demo instance is cold-spilled to a file store.
    fn cold_file_journal(path: &std::path::Path) -> (Journal, Key) {
        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(Some(path)).unwrap());
        journal.set_cold_spill(store, 1, 0);
        let key = deploy_and_create(&mut journal);
        assert_eq!(journal.force_cold_spill_all(), 1);
        assert!(journal.instance(key).is_none());
        (journal, key)
    }

    #[test]
    fn corrupt_spill_row_withholds_activation_and_keeps_the_row() {
        let path = restart_store_path("corrupt-spill-1388");
        let (mut journal, keys) = spilled_journal(&path, 1);
        corrupt_row(&path, "spill", "vars", keys[0]);

        let jobs = journal.activate_jobs("demo-work", "w", 1, 60_000, 0);
        assert!(jobs.is_empty(), "no job handed out with empty variables");
        assert_eq!(raw_row_count(&path, "spill", keys[0]), 1, "row survives");
        assert!(
            journal.engine.is_variables_spilled(keys[0]),
            "still spilled"
        );
        assert!(journal.offheap_read_failures() >= 1, "failure observable");
        assert!(journal.quarantined_orphans().is_empty(), "not an orphan");
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn corrupt_spill_row_rejects_migration_as_retryable_and_keeps_the_row() {
        // Migration/modification restore the spilled root before planning (#1374).
        // An UNREADABLE row must fail closed exactly like activation (#1388):
        // reject as retryable, never run against an instance missing its
        // variables, never destroy the row.
        let path = restart_store_path("corrupt-spill-migrate-1374");
        let (mut journal, keys) = spilled_journal(&path, 1);
        corrupt_row(&path, "spill", "vars", keys[0]);

        let result = journal.apply_command(Command::migrate_instance(keys[0], 0, Vec::new()));
        assert!(
            matches!(
                result,
                Err(EngineError::InstanceUnavailable { instance_key }) if instance_key == keys[0]
            ),
            "migration of an unreadable spilled instance is rejected as retryable"
        );
        assert_eq!(raw_row_count(&path, "spill", keys[0]), 1, "row survives");
        assert!(
            journal.engine.is_variables_spilled(keys[0]),
            "still spilled"
        );
        assert!(journal.quarantined_orphans().is_empty(), "not an orphan");
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn resident_activated_job_rehydrates_a_respilled_healthy_instance() {
        // `apply_command_at` re-spills right after activation; the worker-facing
        // guard must rehydrate rather than withhold a healthy job (#1388).
        let path = restart_store_path("respill-guard-1388");
        let (mut journal, keys) = spilled_journal(&path, 1);
        let activated = journal.engine.activate_jobs("demo-work", "w", 1, 60_000, 0);
        assert_eq!(activated.len(), 1, "precondition: activated while spilled");
        assert!(journal.engine.is_variables_spilled(keys[0]));
        let job = journal
            .resident_activated_job(activated[0].key)
            .expect("healthy re-spilled job is rehydrated, not withheld");
        assert!(!journal.engine.is_variables_spilled(keys[0]));
        assert!(
            !job.variables.is_empty(),
            "job carries its restored variables"
        );
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn unreadable_spill_store_withholds_activation() {
        let path = restart_store_path("broken-spill-1388");
        let (mut journal, keys) = spilled_journal(&path, 1);
        break_store(&path);

        let jobs = journal.activate_jobs("demo-work", "w", 1, 60_000, 0);
        assert!(jobs.is_empty(), "no job handed out with empty variables");
        assert!(journal.engine.is_variables_spilled(keys[0]));
        assert!(journal.offheap_read_failures() >= 1);
        assert!(journal.quarantined_orphans().is_empty());
        // Nothing handed out through the server-facing guard either.
        let job_key = journal
            .engine
            .state()
            .jobs
            .values()
            .find(|j| j.instance_key == keys[0])
            .map(|j| j.key)
            .unwrap();
        assert!(journal.resident_activated_job(job_key).is_none());
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1384 (review): under replicated (Raft) application the same command
    // applied on two replicas whose spill state differs (one still has the row,
    // the other lost it) must produce identical journals — the orphan replica
    // must NOT mint a VariablesLost incident key / wall-clock timestamp there.
    #[test]
    fn replicated_apply_with_differing_spill_state_is_deterministic() {
        let old_path = restart_store_path("replica-determinism-1384-a");
        let (journal, keys) = spilled_journal(&old_path, 1);
        let snapshot = journal.engine.snapshot();
        drop(journal);
        let empty_path = restart_store_path("replica-determinism-1384-b");

        let mut with_row = reboot_from(&old_path, snapshot.clone());
        let mut orphaned = reboot_from(&empty_path, snapshot);
        with_row.set_replicated_apply(true);
        orphaned.set_replicated_apply(true);

        let (ev_a, _) = with_row
            .apply_command_at(Command::activate_jobs("demo-work", "w", 1, 60_000, 7), 7)
            .unwrap();
        let (ev_b, _) = orphaned
            .apply_command_at(Command::activate_jobs("demo-work", "w", 1, 60_000, 7), 7)
            .unwrap();
        assert_eq!(
            orphaned.quarantined_orphans(),
            vec![keys[0]],
            "loss observed"
        );
        assert_eq!(ev_a, ev_b, "identical replicated events");
        assert!(orphaned.incidents().is_empty(), "no replica-local incident");
        assert_eq!(with_row.incidents().len(), orphaned.incidents().len());
        // The next replicated command mints the same keys on both replicas.
        let (next_a, _) = with_row
            .apply_command_at(Command::create_instance("demo"), 8)
            .unwrap();
        let (next_b, _) = orphaned
            .apply_command_at(Command::create_instance("demo"), 8)
            .unwrap();
        assert_eq!(next_a, next_b, "no replica-local key minted");
        drop((with_row, orphaned));
        let _ = std::fs::remove_dir_all(old_path.parent().unwrap());
        let _ = std::fs::remove_dir_all(empty_path.parent().unwrap());
    }

    #[test]
    fn absent_spill_row_on_activation_is_quarantined() {
        let path = restart_store_path("absent-spill-1388");
        let (mut journal, keys) = spilled_journal(&path, 1);
        let store = crate::varspill::VarSpillStore::open(Some(&path)).unwrap();
        store.forget(&[keys[0]]).unwrap();

        let _ = journal.activate_jobs("demo-work", "w", 1, 60_000, 0);
        assert_eq!(journal.quarantined_orphans(), vec![keys[0]]);
        assert!(!journal.engine.is_variables_spilled(keys[0]));
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn corrupt_cold_row_keeps_instance_routable() {
        let path = restart_store_path("corrupt-cold-1388");
        let (mut journal, key) = cold_file_journal(&path);
        corrupt_row(&path, "cold", "snapshot", key);

        assert_eq!(
            journal.apply_command(Command::cancel_instance(key)).err(),
            Some(EngineError::InstanceUnavailable { instance_key: key }),
            "unreadable cold row must fail the command closed, never NotFound"
        );
        assert_eq!(journal.cold_count(), 1, "cold index entry kept");
        assert_eq!(raw_row_count(&path, "cold", key), 1, "cold row kept");
        assert!(journal.offheap_read_failures() >= 1, "failure observable");
        assert!(journal.quarantined_orphans().is_empty());
        let mut live = std::collections::HashSet::new();
        journal.collect_live_instance_keys(&mut live);
        assert!(live.contains(&key), "instance not dropped");
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn unreadable_oldest_cold_row_does_not_starve_healthy_activation() {
        // max_jobs = 1 with an unreadable OLDEST cold row: the poll must skip it
        // (row + routing kept) and still reach the healthy later instance (#1388).
        let path = restart_store_path("starve-cold-1388");
        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(Some(&path)).unwrap());
        journal.set_cold_spill(store, 1, 0);
        let bad = deploy_and_create(&mut journal);
        let good = deploy_and_create(&mut journal);
        assert!(bad < good, "precondition: bad row is the oldest");
        assert_eq!(journal.force_cold_spill_all(), 2);
        corrupt_row(&path, "cold", "snapshot", bad);

        for _ in 0..2 {
            let jobs = journal.activate_jobs("demo-work", "w", 1, 60_000, 0);
            if !jobs.is_empty() {
                assert_eq!(jobs[0].instance_key, good);
                assert_eq!(raw_row_count(&path, "cold", bad), 1, "bad row kept");
                drop(journal);
                let _ = std::fs::remove_dir_all(path.parent().unwrap());
                return;
            }
        }
        panic!("healthy cold instance starved behind an unreadable oldest row");
    }

    /// Two replicas of the same state, both cold-spilled to their own file
    /// store, applying under replicated (Raft) semantics (#1470). `model`
    /// builds the process; returns (healthy, impaired, instance keys, paths).
    fn replica_pair(
        tag: &str,
        model: fn() -> nanobpmn_engine_core::ProcessDefinition,
        n: usize,
    ) -> (Journal, Journal, Vec<Key>, Vec<std::path::PathBuf>) {
        let mut out = Vec::new();
        let mut paths = Vec::new();
        for side in ["healthy", "impaired"] {
            let path = restart_store_path(&format!("{tag}-{side}"));
            let mut journal = Journal::in_memory();
            let store = Arc::new(crate::varspill::VarSpillStore::open(Some(&path)).unwrap());
            journal.set_cold_spill(store, 1, 0);
            let _ = journal
                .apply_command(Command::DeployProcess(model()))
                .unwrap();
            let mut keys = Vec::new();
            for _ in 0..n {
                let (events, _) = journal
                    .apply_command_at(Command::create_instance(model().id.clone()), 0)
                    .unwrap();
                keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
            }
            assert_eq!(journal.force_cold_spill_all(), n);
            journal.set_replicated_apply(true);
            out.push((journal, keys));
            paths.push(path);
        }
        let (impaired, keys) = out.pop().unwrap();
        let (healthy, healthy_keys) = out.pop().unwrap();
        assert_eq!(keys, healthy_keys, "replicas mint identical keys");
        (healthy, impaired, keys, paths)
    }

    // #1470: under Raft apply an unreadable cold activation candidate must fail
    // the entry closed instead of activating a different job set than a
    // healthy replica applying the same committed entry.
    #[test]
    fn replicated_activate_jobs_fails_closed_on_unreadable_cold_candidate() {
        let (mut healthy, mut impaired, keys, paths) = replica_pair("raft-act-1470", demo, 2);
        corrupt_row(&paths[1], "cold", "snapshot", keys[0]);
        let cmd = Command::activate_jobs("demo-work", "w", 1, 60_000, 0);

        let (events, _) = healthy.apply_command(cmd.clone()).unwrap();
        assert!(
            !events.is_empty(),
            "healthy replica activates the oldest job"
        );
        assert_eq!(
            impaired.apply_command(cmd).err(),
            Some(EngineError::InstanceUnavailable {
                instance_key: keys[0]
            }),
            "impaired replica must fail closed, not activate a different job"
        );
        assert_eq!(
            impaired.cold_count(),
            2,
            "nothing applied on the impaired replica"
        );
        drop((healthy, impaired));
        for p in paths {
            let _ = std::fs::remove_dir_all(p.parent().unwrap());
        }
    }

    // #1470: an unreadable candidate BEYOND the `max_jobs` cutoff must still
    // fail closed — selection order spans every candidate, so stopping early
    // could let replicas with differing residency activate different jobs.
    #[test]
    fn replicated_activate_jobs_fails_closed_on_unreadable_candidate_beyond_cutoff() {
        let (mut healthy, mut impaired, keys, paths) = replica_pair("raft-act-cut-1470", demo, 2);
        corrupt_row(&paths[1], "cold", "snapshot", keys[1]);
        let cmd = Command::activate_jobs("demo-work", "w", 1, 60_000, 0);

        let (events, _) = healthy.apply_command(cmd.clone()).unwrap();
        assert!(!events.is_empty(), "healthy replica activates a job");
        assert_eq!(
            impaired.apply_command(cmd).err(),
            Some(EngineError::InstanceUnavailable {
                instance_key: keys[1]
            }),
            "impaired replica must read past the cutoff and fail closed"
        );
        drop((healthy, impaired));
        for p in paths {
            let _ = std::fs::remove_dir_all(p.parent().unwrap());
        }
    }

    fn timed_demo() -> nanobpmn_engine_core::ProcessDefinition {
        ProcessBuilder::new("timed")
            .start_event("start")
            .service_task("work", "demo-work")
            .timer_boundary_event("timeout", "work", 10)
            .end_event("end")
            .end_event("timed-out")
            .connect("start", "work")
            .connect("work", "end")
            .connect("timeout", "timed-out")
            .build()
            .expect("valid timed process")
    }

    // #1470: the same shape for a due cold timer under Raft apply.
    #[test]
    fn replicated_trigger_timers_fails_closed_on_unreadable_due_cold_timer() {
        let (mut healthy, mut impaired, keys, paths) =
            replica_pair("raft-timer-1470", timed_demo, 1);
        corrupt_row(&paths[1], "cold", "snapshot", keys[0]);
        let cmd = Command::TriggerTimers { now: 1_000 };

        let (events, _) = healthy.apply_command(cmd.clone()).unwrap();
        assert!(!events.is_empty(), "healthy replica fires the due timer");
        assert_eq!(
            impaired.apply_command(cmd).err(),
            Some(EngineError::InstanceUnavailable {
                instance_key: keys[0]
            }),
            "impaired replica must fail closed, not skip the due timer"
        );
        assert_eq!(impaired.cold_count(), 1, "timer stays cold and armed");
        drop((healthy, impaired));
        for p in paths {
            let _ = std::fs::remove_dir_all(p.parent().unwrap());
        }
    }

    // #1483: the Raft tick pre-check gates proposing `TriggerTimers` on "is
    // anything due". Gating on HOT timers alone never proposes the command when
    // every due timer is cold-spilled, and `TriggerTimers` is the only path that
    // rehydrates them — the timers never fire. `Journal::has_due_timers` is the
    // hot ∪ cold probe that gate must use.
    #[test]
    fn has_due_timers_covers_cold_spilled_instances() {
        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(None).expect("in-memory store"));
        journal.set_cold_spill(store, 1, 0);
        let _ = journal
            .apply_command(Command::DeployProcess(timed_demo()))
            .unwrap();
        let (events, _) = journal
            .apply_command_at(Command::create_instance("timed"), 0)
            .unwrap();
        let key = events.iter().find_map(|e| e.instance_key()).unwrap();

        // Hot: the timer (due at 10) is resident and visible to the probe.
        assert!(journal.instance(key).is_some(), "instance is hot");
        assert!(!journal.has_due_timers(9), "not yet due");
        assert!(journal.has_due_timers(10), "hot timer due at boundary");

        // Cold: the probe must still see the due timer through the routing
        // index, and proposing the command must rehydrate the instance and
        // fire the timer.
        assert_eq!(journal.force_cold_spill_all(), 1, "instance spills cold");
        assert!(journal.instance(key).is_none(), "instance is cold");
        assert!(
            journal.has_due_timers(10),
            "cold timer still visible to the tick pre-check"
        );
        let (events, _) = journal
            .apply_command_at(Command::TriggerTimers { now: 10 }, 10)
            .unwrap();
        assert!(!events.is_empty(), "the cold timer fires once triggered");
        assert!(journal.instance(key).is_some(), "firing rehydrated it");
        // The fired timer is retained (never re-fires); the probe must no
        // longer see anything due — no hot timer and no cold index entry.
        assert!(!journal.has_due_timers(u64::MAX), "no armed timer remains");
    }

    #[test]
    fn unreadable_cold_store_keeps_instance_routable() {
        let path = restart_store_path("broken-cold-1388");
        let (mut journal, key) = cold_file_journal(&path);
        break_store(&path);

        let _ = journal.activate_jobs("demo-work", "w", 1, 60_000, 0);
        assert_eq!(
            journal.apply_command(Command::cancel_instance(key)).err(),
            Some(EngineError::InstanceUnavailable { instance_key: key }),
            "unreadable store fails the command closed, never NotFound"
        );
        assert_eq!(journal.cold_count(), 1, "cold index entry kept");
        assert!(journal.offheap_read_failures() >= 2);
        assert!(journal.quarantined_orphans().is_empty());
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn rehydrated_cold_row_is_forgotten_only_after_restore() {
        let path = restart_store_path("ok-cold-1388");
        let (mut journal, key) = cold_file_journal(&path);
        let jobs = journal.activate_jobs("demo-work", "w", 1, 60_000, 0);
        assert_eq!(jobs.len(), 1);
        assert!(journal.instance(key).is_some());
        assert_eq!(raw_row_count(&path, "cold", key), 0, "row forgotten");
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    #[test]
    fn absent_cold_row_is_quarantined() {
        let path = restart_store_path("absent-cold-1388");
        let (mut journal, key) = cold_file_journal(&path);
        let store = crate::varspill::VarSpillStore::open(Some(&path)).unwrap();
        store.forget(&[key]).unwrap();

        let _ = journal.apply_command(Command::cancel_instance(key));
        assert_eq!(journal.cold_count(), 0);
        assert_eq!(journal.quarantined_orphans(), vec![key]);
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// Breaks the store at `path` so every read errors (drops both tables) —
    /// the *unreadable* (transient-class) failure, as opposed to an absent row.
    fn break_store(path: &std::path::Path) {
        let conn = rusqlite::Connection::open(path).unwrap();
        conn.execute_batch("DROP TABLE spill; DROP TABLE cold;")
            .unwrap();
    }

    // #1383: a spilled instance whose variable row is DEFINITIVELY absent (the
    // store reads fine, the row is gone) must not block snapshots forever — its
    // variables are already lost, and failing closed only stalled compaction for
    // every other instance (23 days, 5.5 GiB on merlin). The fold quarantines
    // it: snapshotted resident with an empty variable set; every other spilled
    // instance keeps its payload.
    #[test]
    fn try_engine_snapshot_quarantines_missing_spill_row() {
        use nanobpmn_engine_core::Value;

        let path = restart_store_path("orphan-spill");
        let (journal, keys) = spilled_journal(&path, 2);
        let store = Arc::new(crate::varspill::VarSpillStore::open(Some(&path)).unwrap());
        store.forget(&[keys[0]]).unwrap();

        assert!(
            journal.try_engine_snapshot().is_none(),
            "the strict (Raft) capture must not publish a replica-local orphan"
        );
        assert!(journal.quarantined_orphans().is_empty());
        let snap = journal
            .try_engine_snapshot_quarantining()
            .expect("a definitive orphan must not block the local rotation");
        let orphan = &snap.state.instances[&keys[0]];
        assert!(
            !orphan.variables_spilled,
            "orphan no longer flagged spilled"
        );
        assert!(
            orphan.variables.is_empty(),
            "orphan's lost variables stay empty"
        );
        let healthy = &snap.state.instances[&keys[1]];
        assert_eq!(
            healthy.variables.get("data"),
            Some(&Value::Str("payload-1".to_string()))
        );
        assert_eq!(journal.quarantined_orphans(), vec![keys[0]]);
        // Repeated ticks keep succeeding (no retry-forever).
        assert!(journal.try_engine_snapshot_quarantining().is_some());

        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // Copilot review #1332 / #1383: an UNREADABLE spill store (SQLite error —
    // possibly transient) must still fail closed so the rotation aborts before
    // the seal and the next tick retries; only a definitive absence is
    // quarantined.
    #[test]
    fn try_engine_snapshot_fails_closed_on_unreadable_spill_store() {
        let path = restart_store_path("fail-closed-spill");
        let (journal, _keys) = spilled_journal(&path, 2);
        break_store(&path);

        assert!(
            journal.try_engine_snapshot().is_none(),
            "snapshot rotation must abort when a spilled payload cannot be read"
        );
        assert!(journal.quarantined_orphans().is_empty());
        let _ = journal.engine_snapshot();

        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1385: the #1383 failure mode (an unreadable payload fail-closing every
    // snapshot) must NAME the blocking instance key for /health, log the
    // blocker once rather than every tick, and clear once the store reads again
    // — proven across a store outage and its repair, deterministically.
    #[test]
    fn unreadable_spill_store_names_blocker_dedupes_logs_and_clears_on_recovery() {
        let path = restart_store_path("blocker-spill");
        let (journal, keys) = spilled_journal(&path, 2);
        let conn = rusqlite::Connection::open(&path).unwrap();
        conn.execute_batch("ALTER TABLE spill RENAME TO spill_offline;")
            .unwrap();

        for _ in 0..3 {
            assert!(journal.try_engine_snapshot_quarantining().is_none());
        }
        let blockers = journal.snapshot_blockers();
        assert_eq!(blockers.len(), 1, "the fold aborts at the first blocker");
        assert!(
            keys.contains(&blockers[0]),
            "{blockers:?} names a real instance"
        );
        assert_eq!(
            journal.snapshot_blocker_logs_suppressed(),
            2,
            "the same blocker is logged once, then only counted"
        );
        assert!(
            journal.quarantined_orphans().is_empty(),
            "unreadable != orphan"
        );

        conn.execute_batch("ALTER TABLE spill_offline RENAME TO spill;")
            .unwrap();
        assert!(journal.try_engine_snapshot_quarantining().is_some());
        assert!(journal.snapshot_blockers().is_empty(), "cleared on success");

        // A recurrence after recovery is a NEW incident and logs again.
        conn.execute_batch("ALTER TABLE spill RENAME TO spill_offline;")
            .unwrap();
        assert!(journal.try_engine_snapshot_quarantining().is_none());
        assert_eq!(journal.snapshot_blocker_logs_suppressed(), 2);

        drop(conn);
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1383 regression: a snapshot carried over from a pre-#1331 binary holds an
    // instance with `variables_spilled = true` and its empty placeholder, while
    // the spill store was wiped. Recovery must (a) reconcile it at boot —
    // quarantining it with a cleared flag — and (b) snapshot successfully, both
    // before and after reconciliation.
    #[test]
    fn recovered_spilled_orphan_is_reconciled_and_does_not_block_snapshots() {
        let old_path = restart_store_path("orphan-boot-old");
        let (journal, keys) = spilled_journal(&old_path, 2);
        // The raw (unfolded) engine snapshot is what a pre-#1331 build persisted.
        let legacy = journal.engine.snapshot();
        assert!(legacy.state.instances[&keys[0]].variables_spilled);
        drop(journal);

        // Boot over a fresh, empty spill store (pre-#1331 wiped it at boot).
        let new_path = restart_store_path("orphan-boot-new");
        let mut rebooted = reboot_from(&new_path, legacy);
        assert!(
            rebooted.try_engine_snapshot_quarantining().is_some(),
            "orphans must not block the snapshot even before reconciliation"
        );

        let mut orphans = rebooted.reconcile_offheap_orphans();
        orphans.sort_unstable();
        let mut expected = keys.clone();
        expected.sort_unstable();
        assert_eq!(orphans, expected, "every spilled orphan is quarantined");
        for key in &keys {
            assert!(!rebooted.engine.is_variables_spilled(*key));
        }
        assert_eq!(rebooted.quarantined_orphans(), expected);
        assert!(
            rebooted.reconcile_offheap_orphans().is_empty(),
            "reconciliation is idempotent"
        );
        assert!(rebooted.try_engine_snapshot().is_some());

        drop(rebooted);
        let _ = std::fs::remove_dir_all(old_path.parent().unwrap());
        let _ = std::fs::remove_dir_all(new_path.parent().unwrap());
    }

    // #1384: a quarantined off-heap orphan must carry a durable, replicated
    // incident ("variables lost: spill row missing") — the loss is today only
    // logged, never recorded on the instance, so an operator cannot see it via
    // the incidents API. The incident must survive snapshot+recovery (it is
    // journaled engine state, not a host-side note).
    #[test]
    fn quarantined_orphan_carries_a_durable_incident() {
        let old_path = restart_store_path("orphan-incident-old");
        let (journal, keys) = spilled_journal(&old_path, 2);
        let legacy = journal.engine.snapshot();
        drop(journal);

        let new_path = restart_store_path("orphan-incident-new");
        let mut rebooted = reboot_from(&new_path, legacy);
        let mut orphans = rebooted.reconcile_offheap_orphans();
        orphans.sort_unstable();
        let mut expected = keys.clone();
        expected.sort_unstable();
        assert_eq!(orphans, expected, "every spilled orphan is quarantined");

        // Every quarantined orphan carries an active VariablesLost incident.
        let active = rebooted.engine.active_incidents();
        let mut incidented: Vec<Key> = active
            .iter()
            .filter(|i| i.kind == nanobpmn_engine_core::IncidentKind::VariablesLost)
            .map(|i| i.instance_key)
            .collect();
        incidented.sort_unstable();
        assert_eq!(
            incidented, expected,
            "each quarantined orphan must carry an active VariablesLost incident"
        );
        for key in &expected {
            let incident = active
                .iter()
                .find(|i| i.instance_key == *key)
                .expect("orphan incident present");
            assert_eq!(incident.state, nanobpmn_engine_core::IncidentState::Active);
            assert!(
                incident.reason.contains("spill row missing"),
                "incident reason names the loss, got: {}",
                incident.reason
            );
        }

        // The incident is durable journaled state: it survives a snapshot+recovery
        // round-trip (crash/restart), still active, still pinned to the instance.
        let snap = rebooted
            .try_engine_snapshot()
            .expect("snapshot succeeds after quarantine");
        let restart_path = restart_store_path("orphan-incident-restart");
        let restarted = reboot_from(&restart_path, snap);
        let active_after = restarted.engine.active_incidents();
        let mut incidented_after: Vec<Key> = active_after
            .iter()
            .filter(|i| i.kind == nanobpmn_engine_core::IncidentKind::VariablesLost)
            .map(|i| i.instance_key)
            .collect();
        incidented_after.sort_unstable();
        assert_eq!(
            incidented_after, expected,
            "the VariablesLost incident survives snapshot+recovery"
        );

        drop(rebooted);
        drop(restarted);
        let _ = std::fs::remove_dir_all(old_path.parent().unwrap());
        let _ = std::fs::remove_dir_all(new_path.parent().unwrap());
        let _ = std::fs::remove_dir_all(restart_path.parent().unwrap());
    }

    // #1384 durability: the `VariablesLost` incident is *journaled* (persisted to
    // the event log by `apply_command_at` → `persist`), not just held in memory.
    // After the boot reconcile raises it on a file-backed journal, the persisted
    // journal contains the `IncidentRaised` frame, and replaying that frame into a
    // fresh engine reconstructs the identical incident (the follower-replay /
    // crash-recovery contract the issue requires).
    #[test]
    fn quarantine_incident_is_journaled_and_replays() {
        // Build a legacy snapshot holding a spilled orphan (variables_spilled=true
        // over a store whose row is then wiped).
        let old_path = restart_store_path("orphan-journal-old");
        let (journal, keys) = spilled_journal(&old_path, 1);
        let legacy = journal.engine.snapshot();
        let key = keys[0];
        drop(journal);

        // Restore the orphan into a FILE-BACKED journal and reconcile: the
        // incident must be raised AND persisted to the journal on disk.
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-orphan-incident-journal-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        let journal_path = dir.join("test.journal");
        let new_path = restart_store_path("orphan-journal-new");
        {
            let mut journal = Journal::open(&journal_path).unwrap();
            let store = Arc::new(crate::varspill::VarSpillStore::open(Some(&new_path)).unwrap());
            journal.set_spill(store, usize::MAX);
            journal.restore_engine_from_snapshot(legacy);
            assert!(
                journal.engine.is_variables_spilled(key),
                "precondition: orphan"
            );
            assert_eq!(journal.reconcile_offheap_orphans(), vec![key]);
            // The reconcile persists synchronously through the writer; drop to
            // flush+close before reading the file back.
        }

        // The durable journal on disk contains the IncidentRaised frame.
        let persisted = Journal::read_events(&journal_path).unwrap();
        let raised: Vec<_> = persisted
            .iter()
            .filter(|e| {
                matches!(
                    e,
                    Event::IncidentRaised {
                        kind: nanobpmn_engine_core::IncidentKind::VariablesLost,
                        instance_key,
                        ..
                    } if *instance_key == key
                )
            })
            .collect();
        assert_eq!(
            raised.len(),
            1,
            "the VariablesLost IncidentRaised is persisted to the journal"
        );

        // Replaying the persisted journal reconstructs the incident on a fresh
        // engine (follower replay / crash recovery), identical to the leader's.
        let replayed = nanobpmn_engine_core::Engine::replay(persisted);
        let active = replayed.active_incidents();
        assert_eq!(active.len(), 1);
        assert_eq!(
            active[0].kind,
            nanobpmn_engine_core::IncidentKind::VariablesLost
        );
        assert_eq!(active[0].instance_key, key);
        assert!(active[0].reason.contains("spill row missing"));

        let _ = std::fs::remove_dir_all(&dir);
        let _ = std::fs::remove_dir_all(old_path.parent().unwrap());
        let _ = std::fs::remove_dir_all(new_path.parent().unwrap());
    }

    // #1383: reconciliation leaves a spilled instance whose row EXISTS alone.
    #[test]
    fn reconcile_keeps_healthy_spilled_instances() {
        let path = restart_store_path("reconcile-healthy");
        let (mut journal, keys) = spilled_journal(&path, 2);
        assert!(journal.reconcile_offheap_orphans().is_empty());
        for key in &keys {
            assert!(journal.engine.is_variables_spilled(*key));
        }
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1383 (cold tier): a cold instance whose row is definitively absent is
    // quarantined (omitted) rather than blocking every classic and lean
    // snapshot forever.
    #[test]
    fn snapshot_quarantines_missing_cold_row() {
        let mut journal = cold_journal();
        let key = deploy_and_create(&mut journal);
        assert!(
            journal.force_cold_spill_all() >= 1,
            "precondition: went cold"
        );
        journal
            .cold
            .as_ref()
            .expect("cold spill configured")
            .store
            .forget(&[key])
            .unwrap();

        assert!(
            journal.try_engine_snapshot().is_none(),
            "the strict (Raft) capture must not omit a replica-local cold orphan"
        );
        let snap = journal
            .try_engine_snapshot_quarantining()
            .expect("classic snapshot proceeds past a definitive cold orphan");
        assert!(!snap.state.instances.contains_key(&key));
        assert!(
            journal.lean_snapshot_and_cold_upserts().is_some(),
            "lean checkpoint proceeds past a definitive cold orphan"
        );
        assert_eq!(journal.quarantined_orphans(), vec![key]);
    }

    // Copilot review #1332 / #1383 (cold tier): an UNREADABLE cold store still
    // aborts both the classic fold and the lean checkpoint fold.
    #[test]
    fn try_engine_snapshot_fails_closed_on_unreadable_cold_store() {
        let path = restart_store_path("fail-closed-cold");
        let mut journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(Some(&path)).unwrap());
        journal.set_cold_spill(store, 1, 0);
        let _key = deploy_and_create(&mut journal);
        assert!(
            journal.force_cold_spill_all() >= 1,
            "precondition: went cold"
        );
        break_store(&path);

        assert!(
            journal.try_engine_snapshot().is_none(),
            "classic snapshot must abort when a cold instance cannot be read"
        );
        assert!(
            journal.lean_snapshot_and_cold_upserts().is_none(),
            "lean checkpoint must abort when a cold instance cannot be read"
        );
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // Copilot review on #1387: a legacy spilled instance whose resident map
    // already holds journal-tail variable updates must KEEP them when the
    // orphan is quarantined — both in the rotation fold and at boot.
    #[test]
    fn quarantine_preserves_surviving_resident_variables() {
        use nanobpmn_engine_core::Value;

        let old_path = restart_store_path("orphan-tail-old");
        let (journal, keys) = spilled_journal(&old_path, 1);
        let mut legacy = journal.engine.snapshot();
        drop(journal);
        let key = keys[0];
        let inst = legacy.state.instances.get_mut(&key).unwrap();
        assert!(inst.variables_spilled);
        let mut tail = HashMap::new();
        tail.insert("late".to_string(), Value::Str("tail-update".to_string()));
        inst.variables = Arc::new(tail);

        let new_path = restart_store_path("orphan-tail-new");
        let mut rebooted = reboot_from(&new_path, legacy);
        let snap = rebooted.try_engine_snapshot_quarantining().unwrap();
        assert_eq!(
            snap.state.instances[&key].variables.get("late"),
            Some(&Value::Str("tail-update".to_string())),
            "rotation fold keeps surviving resident variables"
        );
        assert_eq!(rebooted.reconcile_offheap_orphans(), vec![key]);
        assert_eq!(
            rebooted.engine.state().instances[&key]
                .variables
                .get("late"),
            Some(&Value::Str("tail-update".to_string())),
            "boot reconciliation keeps surviving resident variables"
        );

        drop(rebooted);
        let _ = std::fs::remove_dir_all(old_path.parent().unwrap());
        let _ = std::fs::remove_dir_all(new_path.parent().unwrap());
    }

    // #1384 review: a HEALTHY spilled instance (row present) also has its
    // snapshot-copy flag cleared by the fold; it must not be mistaken for an
    // orphan and must not raise a VariablesLost incident.
    #[test]
    fn healthy_spill_snapshot_raises_no_incident() {
        let path = restart_store_path("healthy-snap-1384");
        let (mut journal, keys) = spilled_journal(&path, 1);
        assert!(journal.engine.is_variables_spilled(keys[0]));
        let snap = journal
            .try_engine_snapshot_quarantining_with_incidents()
            .expect("capture succeeds");
        assert!(!snap.state.instances[&keys[0]].variables.is_empty());
        assert!(
            journal.engine.active_incidents().is_empty(),
            "no false loss"
        );
        assert!(journal.quarantined_orphans().is_empty());
        drop(journal);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1384 review (Copilot on #1442): raising the quarantine incident must NOT
    // run the per-command spill housekeeping (`maybe_spill`). The orphan's
    // surviving resident variables were just restored for the outer consumer
    // (here: a job activation); a due spill check with an exhausted budget must
    // not re-spill the orphan before that consumer reads them (which would
    // withhold the job / fail the outer command with `InstanceUnavailable`).
    #[test]
    fn orphan_quarantine_does_not_respill_restored_variables() {
        let old_path = restart_store_path("orphan-respill-1384-old");
        let (journal, keys) = spilled_journal(&old_path, 1);
        let legacy = journal.engine.snapshot();
        drop(journal);
        let key = keys[0];

        // Reboot over a store whose row was wiped (orphan), with the spill
        // budget EXHAUSTED (cap 0): any `maybe_spill` re-sheds every resident
        // spill candidate — including a freshly restored orphan.
        let new_path = restart_store_path("orphan-respill-1384-new");
        let mut orphan_journal = Journal::in_memory();
        let store = Arc::new(crate::varspill::VarSpillStore::open(Some(&new_path)).unwrap());
        orphan_journal.set_spill(store, 0);
        orphan_journal.restore_engine_from_snapshot(legacy);
        assert!(orphan_journal.engine.is_variables_spilled(key));

        // Activation discovers the orphan, quarantines it and raises the
        // incident. The incident raise must not re-spill the restored orphan:
        // the job is handed out with the surviving resident variables.
        let activated = orphan_journal.activate_jobs("demo-work", "w", 1, 60_000, 0);
        assert_eq!(activated.len(), 1, "the orphan's job is not withheld");
        assert!(
            !orphan_journal.engine.is_variables_spilled(key),
            "the restored orphan is not re-spilled by the incident raise"
        );
        assert_eq!(orphan_journal.engine.active_incidents().len(), 1);

        // Housekeeping is deferred, not lost: a later ordinary command runs
        // its `maybe_spill` and re-sheds the orphan once it is spillable again
        // (it holds only its empty surviving map until a SetVariables repopulates
        // it, so here we give it a payload first).
        let mut vars = std::collections::HashMap::new();
        vars.insert(
            "data".to_string(),
            nanobpmn_engine_core::Value::Str("surviving".to_string()),
        );
        let _ = orphan_journal
            .apply_command(Command::set_variables(key, vars))
            .unwrap();
        orphan_journal.force_spill_scan();
        assert!(
            orphan_journal.engine.is_variables_spilled(key),
            "the next command's housekeeping re-sheds the orphan"
        );

        drop(orphan_journal);
        let _ = std::fs::remove_dir_all(old_path.parent().unwrap());
        let _ = std::fs::remove_dir_all(new_path.parent().unwrap());
    }

    // #1384 review: snapshot-first quarantine must clear the LIVE spilled flag,
    // so resolving the incident does not re-raise it on the next snapshot.
    #[test]
    fn snapshot_quarantine_then_resolve_does_not_reraise() {
        let old_path = restart_store_path("snap-resolve-1384-old");
        let (journal, keys) = spilled_journal(&old_path, 1);
        let legacy = journal.engine.snapshot();
        drop(journal);
        let new_path = restart_store_path("snap-resolve-1384-new");
        let mut rebooted = reboot_from(&new_path, legacy);
        rebooted
            .try_engine_snapshot_quarantining_with_incidents()
            .expect("capture");
        assert!(
            !rebooted.engine.is_variables_spilled(keys[0]),
            "live flag cleared"
        );
        let incident = rebooted.engine.active_incidents()[0].key;
        let _ = rebooted
            .apply_command_at(Command::resolve_incident(incident), 9)
            .unwrap();
        assert!(rebooted.engine.active_incidents().is_empty());
        rebooted
            .try_engine_snapshot_quarantining_with_incidents()
            .expect("capture");
        assert!(
            rebooted.engine.active_incidents().is_empty(),
            "resolved loss is not re-raised"
        );
        drop(rebooted);
        let _ = std::fs::remove_dir_all(old_path.parent().unwrap());
        let _ = std::fs::remove_dir_all(new_path.parent().unwrap());
    }

    // #1384 review: under replicated apply the rotation capture is strict — it
    // must neither mint a replica-local incident nor mutate live state.
    #[test]
    fn replicated_snapshot_rotation_is_strict_on_orphan() {
        let old_path = restart_store_path("raft-snap-1384-old");
        let (journal, keys) = spilled_journal(&old_path, 1);
        let legacy = journal.engine.snapshot();
        drop(journal);
        let new_path = restart_store_path("raft-snap-1384-new");
        let mut orphaned = reboot_from(&new_path, legacy.clone());
        let mut with_row = reboot_from(&old_path, legacy);
        orphaned.set_replicated_apply(true);
        with_row.set_replicated_apply(true);
        assert!(
            orphaned
                .try_engine_snapshot_quarantining_with_incidents()
                .is_none()
        );
        assert!(
            with_row
                .try_engine_snapshot_quarantining_with_incidents()
                .is_some()
        );
        assert!(orphaned.engine.active_incidents().is_empty());
        assert!(
            orphaned.engine.is_variables_spilled(keys[0]),
            "live state untouched"
        );
        let (a, _) = with_row
            .apply_command_at(Command::create_instance("demo"), 8)
            .unwrap();
        let (b, _) = orphaned
            .apply_command_at(Command::create_instance("demo"), 8)
            .unwrap();
        assert_eq!(a, b, "no replica-local key minted");
        drop((orphaned, with_row));
        let _ = std::fs::remove_dir_all(old_path.parent().unwrap());
        let _ = std::fs::remove_dir_all(new_path.parent().unwrap());
    }

    // #1384 review: a snapshot-first discovery must raise the durable
    // `VariablesLost` incident BEFORE the snapshot seals. Previously the rotation
    // fold only recorded the orphan locally and cleared its spilled flag in the
    // sealed snapshot, so a restart restored the cleared flag with no incident and
    // boot reconciliation never reported the loss. The capture-and-incident path
    // (`try_engine_snapshot_quarantining_with_incidents`) closes that gap.
    #[test]
    fn snapshot_first_discovery_raises_a_durable_incident() {
        let old_path = restart_store_path("orphan-snapfirst-old");
        let (journal, keys) = spilled_journal(&old_path, 1);
        let legacy = journal.engine.snapshot();
        drop(journal);
        let key = keys[0];

        // Reboot over a store whose row was wiped, then take the QUARANTINING
        // capture directly (snapshot discovers the loss before any boot
        // reconciliation runs).
        let new_path = restart_store_path("orphan-snapfirst-new");
        let mut rebooted = reboot_from(&new_path, legacy);
        assert!(
            rebooted.engine.is_variables_spilled(key),
            "precondition: orphan still spilled in live state"
        );

        let snap = rebooted
            .try_engine_snapshot_quarantining_with_incidents()
            .expect("quarantining capture succeeds");

        // The snapshot sealed the orphan with its spilled flag cleared...
        assert!(
            !snap.state.instances[&key].variables_spilled,
            "snapshot clears the lost spill flag"
        );
        // ...AND the live engine now carries the durable incident.
        let active = rebooted.engine.active_incidents();
        let incident = active
            .iter()
            .find(|i| {
                i.instance_key == key && i.kind == nanobpmn_engine_core::IncidentKind::VariablesLost
            })
            .expect("snapshot-first discovery raises the VariablesLost incident");
        assert_eq!(incident.state, nanobpmn_engine_core::IncidentState::Active);
        assert!(incident.reason.contains("spill row missing"));

        // The captured snapshot ALSO carries the incident, so a restart from it
        // restores the report (no dependence on a second reconciliation pass).
        let restart_path = restart_store_path("orphan-snapfirst-restart");
        let restarted = reboot_from(&restart_path, snap);
        let active_after = restarted.engine.active_incidents();
        assert!(
            active_after.iter().any(|i| {
                i.instance_key == key && i.kind == nanobpmn_engine_core::IncidentKind::VariablesLost
            }),
            "the incident survives the snapshot it was raised into"
        );

        drop(rebooted);
        drop(restarted);
        let _ = std::fs::remove_dir_all(old_path.parent().unwrap());
        let _ = std::fs::remove_dir_all(new_path.parent().unwrap());
        let _ = std::fs::remove_dir_all(restart_path.parent().unwrap());
    }

    // Copilot review on #1387: with NO spill store wired, absence cannot be
    // proven — neither the fold nor boot reconciliation may quarantine.
    #[test]
    fn unwired_spill_store_is_not_proof_of_absence() {
        let path = restart_store_path("orphan-unwired");
        let (journal, keys) = spilled_journal(&path, 1);
        let legacy = journal.engine.snapshot();
        drop(journal);
        let mut rebooted = Journal::in_memory();
        rebooted.restore_engine_from_snapshot(legacy);

        assert!(rebooted.try_engine_snapshot_quarantining().is_none());
        assert!(rebooted.reconcile_offheap_orphans().is_empty());
        assert!(rebooted.engine.is_variables_spilled(keys[0]));
        assert!(rebooted.quarantined_orphans().is_empty());

        drop(rebooted);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // Copilot review on #1418: with NO spill store wired the restore is a
    // no-op, so modify/migrate would evaluate against the empty placeholder.
    // Both must fail closed as retryable instead.
    #[test]
    fn unwired_spill_store_rejects_modify_and_migrate_as_retryable() {
        let path = restart_store_path("unwired-modify-migrate-1418");
        let (journal, keys) = spilled_journal(&path, 1);
        let legacy = journal.engine.snapshot();
        drop(journal);
        let mut rebooted = Journal::in_memory();
        rebooted.restore_engine_from_snapshot(legacy);

        for command in [
            Command::migrate_instance(keys[0], 0, Vec::new()),
            Command::modify_instance(keys[0], Vec::new(), Vec::new()),
        ] {
            let result = rebooted.apply_command(command);
            assert!(
                matches!(
                    result,
                    Err(EngineError::InstanceUnavailable { instance_key }) if instance_key == keys[0]
                ),
                "unwired spill store must reject as retryable, got {:?}",
                result.as_ref().err()
            );
            assert!(
                rebooted.engine.is_variables_spilled(keys[0]),
                "still spilled"
            );
        }

        drop(rebooted);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // Copilot review on #1387: lean mode enabled over a legacy spilled
    // snapshot — the new var store misses but the legacy spill row survives.
    // That is not an orphan: fail closed, never clear the flag.
    #[test]
    fn lean_over_legacy_spill_row_is_not_quarantined() {
        let path = restart_store_path("orphan-lean-legacy");
        let (journal, keys) = spilled_journal(&path, 1);
        let legacy = journal.engine.snapshot();
        drop(journal);
        let vpath = restart_store_path("orphan-lean-legacy-vs");
        let mut rebooted = reboot_from(&path, legacy);
        let varstore = Arc::new(crate::varstore::VarStore::open(Some(&vpath)).unwrap());
        rebooted.set_varstore(varstore);

        assert!(rebooted.try_engine_snapshot_quarantining().is_none());
        assert!(rebooted.reconcile_offheap_orphans().is_empty());
        assert!(rebooted.engine.is_variables_spilled(keys[0]));

        drop(rebooted);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        let _ = std::fs::remove_dir_all(vpath.parent().unwrap());
    }

    // Copilot review on #1387: lean mode over a legacy placeholder snapshot
    // with the legacy tier unwired — an empty var store proves nothing.
    #[test]
    fn lean_with_unwired_legacy_tier_is_not_quarantined() {
        let path = restart_store_path("orphan-lean-unwired");
        let (journal, keys) = spilled_journal(&path, 1);
        let legacy = journal.engine.snapshot();
        drop(journal);
        let vpath = restart_store_path("orphan-lean-unwired-vs");
        let mut rebooted = Journal::in_memory();
        rebooted.restore_engine_from_snapshot(legacy);
        let varstore = Arc::new(crate::varstore::VarStore::open(Some(&vpath)).unwrap());
        rebooted.set_varstore(varstore);

        assert!(rebooted.try_engine_snapshot_quarantining().is_none());
        assert!(rebooted.reconcile_offheap_orphans().is_empty());
        assert!(rebooted.engine.is_variables_spilled(keys[0]));
        assert!(rebooted.quarantined_orphans().is_empty());

        drop(rebooted);
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        let _ = std::fs::remove_dir_all(vpath.parent().unwrap());
    }
}
