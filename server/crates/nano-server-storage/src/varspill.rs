//! A disk-backed store for spilled process-instance variables.
//!
//! The dominant cost of a large *active* backlog (instances created and parked
//! on a job, waiting for a worker) is the 50 KB-class `variables` payload each
//! one holds in hot RAM. [`crate::Journal`] moves the variables of such cold
//! instances here, out of the engine, and rehydrates them at job-activation
//! time. This bounds resident memory the way Zeebe's RocksDB-backed state does,
//! while keeping in-memory speed for the working set.
//!
//! The backing store is a single SQLite table. SQLite's page cache (plus the OS
//! page cache underneath it) gives the tiering for free: a working set that fits
//! the cache is served at memory speed, and only a genuinely large spill touches
//! the disk — exactly the "memory/fs fusion" the spill is after, with no manual
//! eviction logic. `synchronous=NORMAL` is safe here because, in the steady
//! state, the spill store is a *derived* cache: a self-contained snapshot
//! (#1331) folds every spilled/cold payload back into the engine state it
//! persists, so the variables are durable in the journal and a lost spill page
//! is reconstructable from it.
//!
//! The one exception is **legacy recovery**: a snapshot written by an engine
//! predating self-contained snapshots records a spilled instance as an empty
//! placeholder, so after log compaction the spill row here is the *only*
//! surviving copy of that instance's variables. Boot therefore never wipes these
//! rows (see [`VarSpillStore::open`]), and a placeholder snapshot rehydrates
//! from them on restart. Until every pre-#1331 snapshot has aged out, treat this
//! store as authoritative for those rows — do not wipe or weaken it.
//!
//! ## Returning freed space to the OS
//!
//! Destructive reads ([`VarSpillStore::take`]) and terminal eviction
//! ([`VarSpillStore::forget`]) keep the *live* row set bounded to the still-cold
//! backlog — but SQLite never returns freed pages to the OS on its own, so the
//! *file* would otherwise plateau at the high-water mark of the largest backlog
//! ever seen (a single spike parks ~payload × peak-backlog on disk for the life of
//! the process — e.g. 100+ GB after one bad soak). The store therefore runs in
//! `INCREMENTAL` auto-vacuum mode so freed pages land on a reclaimable freelist.
//!
//! Reclaiming that freelist is fsync-heavy (`wal_checkpoint(TRUNCATE)` +
//! `incremental_vacuum` copy-back). Doing it inline on the eviction path was a
//! throughput regression: on a shared disk those fsyncs contend with the engine's
//! raft-log fsync and repeatedly trip its ADR-0020 saturation guard, so creates get
//! shed under sustained load. Instead a **background reclaim thread** drains the
//! freelist only while the store is *quiescent* — which is exactly the post-spike
//! moment the reclaim targets (the huge backlog has drained; nothing is spilling or
//! rehydrating). Under steady load the store is never idle, so the thread never
//! fires and adds zero fsync pressure; the file simply tracks the live working set.
//! The drain runs in bounded passes, releasing the connection lock between them, so
//! resuming load preempts it immediately.
//!
//! `NANOBPMN_VARSPILL_RECLAIM_MB` sets the freelist gate (0 disables reclaim
//! entirely, and skips the thread). `NANOBPMN_VARSPILL_RECLAIM_IDLE_MS` sets how
//! long the store must be quiet before a drain begins.

use std::collections::HashMap;
use std::path::Path;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
#[cfg(test)]
use std::sync::mpsc::Receiver;
use std::sync::mpsc::{Sender, SyncSender, TrySendError, channel, sync_channel};
use std::sync::{Arc, Mutex};
use std::thread::JoinHandle;
use std::time::Duration;

use nanobpmn_engine_core::{InstanceSnapshot, Key, Value};
use rusqlite::{Connection, OptionalExtension, params};

use crate::sqlite_space::{
    enable_incremental_auto_vacuum, freelist_bytes, page_stats, reclaim_freelist_step,
};

/// Freelist bytes that must accumulate before the background reclaim thread spends
/// an `incremental_vacuum` + `wal_checkpoint(TRUNCATE)` to return them to the OS.
/// Default 64 MiB — high enough that steady-state churn (bounded live backlog)
/// never crosses it, low enough to cap a drained file near `live + 64 MiB` rather
/// than the historical peak. `NANOBPMN_VARSPILL_RECLAIM_MB` overrides; 0 disables
/// reclaim entirely (pre-fix high-water behaviour, and skips the thread).
fn reclaim_threshold_bytes() -> u64 {
    std::env::var("NANOBPMN_VARSPILL_RECLAIM_MB")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(64)
        * 1024
        * 1024
}

/// How long the store must be free of mutations before the background thread starts
/// draining the freelist, in milliseconds. Keeps reclaim off active-load windows so
/// its fsyncs never contend with the engine's raft-log fsync. Default 2000 ms.
fn reclaim_idle_ms() -> u64 {
    std::env::var("NANOBPMN_VARSPILL_RECLAIM_IDLE_MS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .unwrap_or(2000)
}

/// Keys the forget worker forgets between its own `wal_checkpoint(TRUNCATE)`
/// backstop passes (each key can delete up to two rows — a `spill` row and a
/// `cold` row). With `wal_autocheckpoint=0` the WAL only truncates off the
/// hot path; this bounds WAL growth under sustained retirement (when the idle
/// reclaim thread never runs) without checkpointing so often that its fsync
/// contends with live raft-log fsync. Tunable via NANOBPMN_VARSPILL_WAL_CKPT_KEYS.
fn wal_checkpoint_keys() -> u64 {
    std::env::var("NANOBPMN_VARSPILL_WAL_CKPT_KEYS")
        .ok()
        .and_then(|v| v.trim().parse::<u64>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(262_144)
}

/// Maximum number of pending forget batches the background worker will hold
/// before [`VarSpillStore::forget_async`] falls back to a synchronous delete. Bounds
/// the queue's memory so a sustained producer > consumer imbalance degrades
/// predictably (backpressure onto the enqueuing follower actor) instead of growing
/// without bound. Tunable via NANOBPMN_VARSPILL_FORGET_QUEUE.
fn forget_queue_cap() -> usize {
    std::env::var("NANOBPMN_VARSPILL_FORGET_QUEUE")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .filter(|&n| n > 0)
        .unwrap_or(256)
}

/// Pages reclaimed per bounded background pass (~256 MiB at a 4 KiB page). Small
/// enough that a pass holds the connection lock only briefly (so resuming load
/// preempts a drain), large enough to clear even a 100 GB post-spike freelist within
/// a couple of minutes of idle.
const RECLAIM_CHUNK_PAGES: u32 = 65_536;

/// Keys deleted per background-forget transaction. Small enough that the
/// forget worker holds the connection lock only briefly per commit, so it
/// interleaves with the engine actor's own spill/rehydrate access instead of
/// stalling it with one huge multi-hundred-thousand-row transaction.
const FORGET_CHUNK: usize = 4_096;

/// Background thread that applies follower **retirement** `forget` deletes off the
/// engine actor. The follower retirement backstop can reap up to 100k cold
/// victims per tick; running that DELETE inline on the single-writer replica actor
/// holds the actor in SQLite work every retirement tick and (before
/// `wal_autocheckpoint` was disabled) tripped WAL checkpoints whose fsyncs
/// contended with live raft-log fsync — stalling replication and roughly halving
/// 50 KB-payload throughput. Offloading the deletes here keeps the actor free
/// while still reclaiming the rows, and the worker checkpoints only on its own
/// bounded, infrequent cadence (keys are unique and never reused, so a queued
/// forget can never clobber a later re-spill).
///
/// Steady-state exporter-driven **eviction** ([`crate::journal::Journal::evict_instances`])
/// deliberately stays synchronous ([`VarSpillStore::forget`]): it runs on the owner
/// (not a hot replica), one instance at a time, where immediate deletion keeps the
/// durable store consistent with the read model.
struct ForgetWorker {
    /// `None` after [`Drop`] has closed the channel. The queue is bounded
    /// ([`forget_queue_cap`]); [`VarSpillStore::forget_async`] uses a non-blocking
    /// `try_send` and falls back to a synchronous delete when it is full, so a
    /// slow consumer applies backpressure instead of growing memory unbounded.
    /// The worker drains and deletes in [`FORGET_CHUNK`]s.
    tx: Option<SyncSender<ForgetMsg>>,
    handle: Option<JoinHandle<()>>,
}

/// Work item for the [`ForgetWorker`]: a batch of keys to delete, or a flush
/// barrier that acks once every batch queued before it has been applied (used by
/// tests and graceful shutdown to observe the async deletes deterministically).
enum ForgetMsg {
    Keys(Vec<Key>),
    Flush(Sender<()>),
}

impl Drop for ForgetWorker {
    fn drop(&mut self) {
        // Closing the channel lets the worker drain any queued batches (so no
        // forget is lost on shutdown) and then exit its `recv` loop; join it.
        drop(self.tx.take());
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Background thread that drains the SQLite freelist while the store is quiescent.
struct ReclaimWorker {
    stop: Arc<AtomicBool>,
    handle: Option<JoinHandle<()>>,
}

impl Drop for ReclaimWorker {
    fn drop(&mut self) {
        self.stop.store(true, Ordering::Relaxed);
        if let Some(h) = self.handle.take() {
            let _ = h.join();
        }
    }
}

/// Observable record of a fatal forget/delete failure (#1366). Set exactly once
/// (the first error wins — later faults are almost always the same condition) by
/// the background forget worker or by the synchronous [`VarSpillStore::forget`]
/// fallback, and never cleared: a store that has lost deletes stays faulted for
/// its lifetime so the failure can never be silently forgotten.
///
/// `record()` also **closes producer admission** (sets `closed`): the worker's
/// failure path records the fault *before* it drains the queue, so a
/// `forget_async*` racing the drain either enqueues before the close (and is
/// retained by that drain) or sees the closed flag and takes the synchronous
/// fallback — no accepted batch is ever dropped unrecorded in the shutdown
/// window, and no batch enqueued after the close is left stranded. The worker
/// then stops on the fault (rather than dropping or retrying the batch), so a
/// `Some` error here also means the worker is gone and every batch still queued
/// at that point was drained into [`Self::retained_keys`] — no pending delete is
/// ever dropped silently. Once faulted, later `forget_async*` calls fall back to
/// synchronous deletes, which either succeed (disk healthy again) or latch the
/// same fault and retain their batch here.
#[derive(Debug, Default)]
pub struct ForgetFaults {
    /// The first fatal error (delete or checkpoint), as a display string.
    error: Mutex<Option<String>>,
    /// Keys whose deletes were not applied: the failed batch, every batch
    /// drained from the queue on the worker's way out, and every batch whose
    /// synchronous fallback delete failed afterwards.
    retained_keys: Mutex<Vec<Key>>,
    /// Producer admission is closed: the worker has faulted (or is faulting),
    /// so `forget_async*` must take the synchronous fallback instead of
    /// enqueueing work the exiting worker's drain might miss.
    closed: AtomicBool,
}

impl ForgetFaults {
    /// Latches `err` (first fault wins), closes producer admission, and appends
    /// `retained` to the retained key set. Lock poisons are recovered: a
    /// poisoned latch must never mask the fault it records.
    ///
    /// The close is published (release) *before* the retained keys, so a
    /// producer that observes `closed` and falls back to a failing synchronous
    /// delete appends its own batch after the worker's drain — never into a
    /// gap the drain already passed.
    fn record(&self, err: &rusqlite::Error, retained: Vec<Key>) {
        let mut slot = self.error.lock().unwrap_or_else(|e| e.into_inner());
        if slot.is_none() {
            *slot = Some(err.to_string());
        }
        self.closed.store(true, Ordering::Release);
        drop(slot);
        self.retain(retained);
    }

    /// Appends `retained` keys whose deletes were not applied (a drained queue
    /// batch, or a synchronous-fallback batch whose delete failed). Separate
    /// from [`record`](Self::record) so the worker can close admission and latch
    /// the error first, then append the drained keys.
    fn retain(&self, retained: Vec<Key>) {
        if !retained.is_empty() {
            self.retained_keys
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .extend(retained);
        }
    }

    /// The first latched fault, if any.
    pub fn error(&self) -> Option<String> {
        self.error.lock().unwrap_or_else(|e| e.into_inner()).clone()
    }

    /// Keys whose deletes were retained (not applied): the failed batch, the
    /// drained queue, and any later synchronous-fallback failures.
    pub fn retained_keys(&self) -> Vec<Key> {
        self.retained_keys
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .clone()
    }

    /// Whether producer admission is closed (a fatal fault was recorded).
    /// `forget_async*` checks this *before* enqueueing so a batch can never be
    /// accepted into a queue the faulting worker's drain has already passed.
    fn is_closed(&self) -> bool {
        self.closed.load(Ordering::Acquire)
    }
}

/// Test-only fault-injection seam (#1366): lets a unit test force the forget
/// worker's next delete batch or WAL-truncate checkpoint to fail with a real
/// [`rusqlite::Error`], so the fault-latch path is covered deterministically
/// without the process-global env knobs (which race parallel tests). Compiled
/// out of production builds entirely.
///
/// `gate` additionally lets a test hold the worker at a deterministic point —
/// after it has received a batch, just before the delete runs — so the test can
/// queue the rest of the scenario (more batches, a flush barrier) *before* the
/// failure is allowed to happen, instead of relying on thread scheduling.
#[cfg(test)]
#[derive(Default)]
struct ForgetFaultInjection {
    fail_next_delete: AtomicBool,
    fail_next_checkpoint: AtomicBool,
    /// Gate state: the worker signals `parked_tx` (cloned out at the gate) and
    /// then blocks on `release_rx` until the test lets it proceed.
    gate_parked: Mutex<Option<Sender<()>>>,
    gate_release: Mutex<Option<Receiver<()>>>,
}

#[cfg(test)]
impl ForgetFaultInjection {
    /// The injected delete error for this batch, if armed (disarms on take).
    fn take_delete_error(&self) -> Option<rusqlite::Error> {
        if self.fail_next_delete.swap(false, Ordering::Relaxed) {
            Some(rusqlite::Error::InvalidQuery)
        } else {
            None
        }
    }

    /// The injected checkpoint error, if armed (disarms on take).
    fn take_checkpoint_error(&self) -> Option<rusqlite::Error> {
        if self.fail_next_checkpoint.swap(false, Ordering::Relaxed) {
            Some(rusqlite::Error::InvalidQuery)
        } else {
            None
        }
    }

    /// Arms the worker to fail its next delete, but only after blocking at a
    /// gate until the returned releaser is signalled. The returned receiver
    /// fires once the worker is parked at the gate, so the test can queue the
    /// remaining batches and the flush barrier while the failure is held —
    /// making the queued-work retention assertions deterministic.
    fn arm_gated_delete_failure(&self) -> (Receiver<()>, Sender<()>) {
        let (parked_tx, parked_rx) = channel();
        let (release_tx, release_rx) = channel();
        *self.gate_parked.lock().unwrap_or_else(|e| e.into_inner()) = Some(parked_tx);
        *self.gate_release.lock().unwrap_or_else(|e| e.into_inner()) = Some(release_rx);
        self.fail_next_delete.store(true, Ordering::Relaxed);
        (parked_rx, release_tx)
    }

    /// If a gate is armed, parks the worker (signalling `parked`) until the
    /// test releases it. Runs just before the delete, after the batch is
    /// received, so everything the test queued while the worker was parked is
    /// behind the failed batch in the channel.
    fn wait_at_gate(&self) {
        let parked = self
            .gate_parked
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        let release = self
            .gate_release
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .take();
        if let (Some(parked_tx), Some(release_rx)) = (parked, release) {
            let _ = parked_tx.send(());
            let _ = release_rx.recv();
        }
    }
}

/// A SQLite-backed key → variables map for spilled instance payloads.
pub struct VarSpillStore {
    conn: Arc<Mutex<Connection>>,
    /// Mutation counter shared by [`put`](Self::put) / [`take`](Self::take) /
    /// `forget` / the background forget worker; the reclaim thread treats a stable
    /// count as "idle" and holds off its fsync-heavy vacuum while the store churns.
    activity: Arc<AtomicU64>,
    /// Present iff reclaim is enabled (file-backed store with a non-zero gate).
    /// Held only to keep the background reclaim thread alive; its [`Drop`] stops
    /// and joins the thread.
    #[allow(dead_code)]
    reclaim: Option<ReclaimWorker>,
    /// Present iff file-backed: applies follower *retirement* forgets off the actor
    /// (exporter-driven *eviction* stays on the synchronous `forget` path). Absent
    /// for in-memory stores (tests), where `forget_async` runs inline so assertions
    /// observe the delete synchronously.
    forget: Option<ForgetWorker>,
    /// Latched forget/delete failure state, shared with the forget worker (see
    /// [`ForgetFaults`]). Always present; empty until the first fatal fault.
    faults: Arc<ForgetFaults>,
    /// Test-only fault-injection seam shared with the forget worker.
    #[cfg(test)]
    fault_injection: Arc<ForgetFaultInjection>,
}

impl VarSpillStore {
    /// Opens (creating if absent) the spill store at `path`, or an in-memory
    /// store when `path` is `None` (tests / ephemeral runs).
    pub fn open(path: Option<&Path>) -> rusqlite::Result<Self> {
        Self::open_with_reclaim_threshold(path, reclaim_threshold_bytes())
    }

    /// [`open`](Self::open) with an explicit freelist reclaim gate, so tests can
    /// force reclaim without racing the process-global `NANOBPMN_VARSPILL_RECLAIM_MB`.
    fn open_with_reclaim_threshold(
        path: Option<&Path>,
        reclaim_threshold_bytes: u64,
    ) -> rusqlite::Result<Self> {
        Self::open_with_params(path, reclaim_threshold_bytes, reclaim_idle_ms())
    }

    /// Full-control constructor (gate + idle window), used by tests to drive the
    /// background reclaimer deterministically without touching process-global env.
    fn open_with_params(
        path: Option<&Path>,
        reclaim_threshold_bytes: u64,
        idle_ms: u64,
    ) -> rusqlite::Result<Self> {
        let conn = match path {
            Some(p) => Connection::open(p)?,
            None => Connection::open_in_memory()?,
        };
        conn.execute_batch(
            // wal_autocheckpoint=0 disables SQLite's implicit 1000-page checkpoint.
            // Under sustained retirement the forget worker deletes at a high rate;
            // an auto-checkpoint fires an fsync-heavy `wal_checkpoint` on that
            // thread, and because the spill file shares the physical disk with the
            // raft log, that checkpoint I/O inflates live raft-log fsync latency
            // ~7x (measured), halving create throughput. With auto-checkpoint off,
            // deletes are cheap WAL appends (synchronous=NORMAL never fsyncs a
            // commit); the WAL is truncated off the hot path — by the idle-gated
            // reclaim thread, and by the forget worker's own size-gated backstop
            // (see spawn_forget_worker) so it can never grow without bound.
            "PRAGMA journal_mode=WAL;
             PRAGMA synchronous=NORMAL;
             PRAGMA wal_autocheckpoint=0;
             CREATE TABLE IF NOT EXISTS spill (key INTEGER PRIMARY KEY, vars TEXT NOT NULL);
             CREATE TABLE IF NOT EXISTS cold (key INTEGER PRIMARY KEY, snapshot TEXT NOT NULL);",
        )?;
        // Rows are deliberately NOT wiped at boot (#1331). A snapshot written by
        // an engine predating self-contained snapshots records spilled instances
        // as empty placeholders, so these rows are the only surviving copy of
        // their variables; wiping them destroyed that data on every restart. A
        // leftover row is inert: a spill row is only read for an instance whose
        // live `variables_spilled` flag is set, and a cold row only through the
        // live routing index — both of which a fresh spill re-writes (`put` /
        // `put_cold` replace). Rows for instances that reach a terminal state are
        // dropped by `forget` at eviction, so the store stays bounded by the
        // live set.
        //
        // That bound is **best-effort across crashes** (accepted, #1338): a crash
        // between writing a cold/spill row and the instance reaching a terminal
        // state (or a row written by an engine predating self-contained
        // snapshots, whose routing index was never persisted) leaves an orphaned
        // row that `forget` never reaches, because the in-RAM cold index is not
        // rebuilt at boot. A post-recovery reconciliation/GC that deletes rows no
        // longer referenced by live state — while preserving rows still
        // referenced by legacy spilled flags — is deliberately **not** added in
        // this PR: it carries durability/ordering implications (what is
        // authoritative, when to run it, how to avoid deleting a row a concurrent
        // recovery still needs) and is tracked separately in #1338.
        //
        // Convert to INCREMENTAL auto-vacuum so freed pages can later be handed
        // back to the OS instead of plateauing at the high-water mark (a one-time
        // VACUUM on a store that predates the conversion).
        enable_incremental_auto_vacuum(&conn)?;
        let conn = Arc::new(Mutex::new(conn));
        let activity = Arc::new(AtomicU64::new(0));
        let faults = Arc::new(ForgetFaults::default());
        #[cfg(test)]
        let fault_injection = Arc::new(ForgetFaultInjection::default());

        // Spawn the background reclaim thread only when it can do useful work: an
        // in-memory store has no file to shrink, and a zero gate disables reclaim.
        let reclaim = if path.is_some() && reclaim_threshold_bytes > 0 {
            Some(Self::spawn_reclaim_worker(
                Arc::clone(&conn),
                Arc::clone(&activity),
                reclaim_threshold_bytes,
                idle_ms,
            ))
        } else {
            None
        };
        // Offload follower retirement forgets off the actor for file-backed stores
        // (eviction stays synchronous on `forget`). In-memory stores forget inline
        // (see `forget_async`) so tests stay deterministic and there is no
        // cross-actor contention to relieve.
        let forget = if path.is_some() {
            Some(Self::spawn_forget_worker(
                Arc::clone(&conn),
                Arc::clone(&activity),
                Arc::clone(&faults),
                #[cfg(test)]
                Arc::clone(&fault_injection),
            ))
        } else {
            None
        };
        Ok(Self {
            conn,
            activity,
            reclaim,
            forget,
            faults,
            #[cfg(test)]
            fault_injection,
        })
    }

    /// Starts the idle-gated background freelist drainer. It polls activity; once the
    /// store has been quiet for `idle_ms` and the freelist exceeds `threshold_bytes`,
    /// it reclaims in [`RECLAIM_CHUNK_PAGES`]-sized passes, re-checking for resumed
    /// activity between passes so live load preempts the drain.
    // expect_used: spawning the reclaim thread fails only on OS resource
    // exhaustion (thread spawn); there is no recoverable fallback for a missing
    // maintenance thread at open, so abort startup loudly.
    #[allow(
        clippy::expect_used,
        reason = "thread spawn failure is a fatal OS resource error at store open"
    )]
    fn spawn_reclaim_worker(
        conn: Arc<Mutex<Connection>>,
        activity: Arc<AtomicU64>,
        threshold_bytes: u64,
        idle_ms: u64,
    ) -> ReclaimWorker {
        let stop = Arc::new(AtomicBool::new(false));
        let poll = Duration::from_millis((idle_ms / 2).clamp(100, 1000));
        let idle_ticks = (idle_ms as f64 / poll.as_millis() as f64).ceil().max(1.0) as u32;

        let handle = {
            let activity = Arc::clone(&activity);
            let stop = Arc::clone(&stop);
            std::thread::Builder::new()
                .name("varspill-reclaim".into())
                .spawn(move || {
                    let mut last_seen = activity.load(Ordering::Relaxed);
                    let mut quiet = 0u32;
                    while !stop.load(Ordering::Relaxed) {
                        std::thread::sleep(poll);
                        let now = activity.load(Ordering::Relaxed);
                        if now != last_seen {
                            last_seen = now;
                            quiet = 0;
                            continue;
                        }
                        quiet = quiet.saturating_add(1);
                        if quiet < idle_ticks {
                            continue;
                        }
                        // Quiescent: drain one bounded pass. Skip cheaply if there is
                        // nothing worth a copy-back. Any activity during/after the
                        // pass resets the idle counter above on the next tick.
                        // Recover a poisoned lock (another thread panicked
                        // mid-mutation; the connection is still usable because every
                        // op is atomic under the lock) rather than exiting the
                        // worker — matching `delete_keys` and the forget worker. A
                        // silent exit here would stop freelist reclaim while
                        // foreground writes continue (#1366).
                        let conn = conn.lock().unwrap_or_else(|e| e.into_inner());
                        if freelist_bytes(&conn) >= threshold_bytes {
                            let _ = reclaim_freelist_step(&conn, RECLAIM_CHUNK_PAGES);
                        }
                    }
                })
                .expect("spawn varspill-reclaim thread")
        };
        ReclaimWorker {
            stop,
            handle: Some(handle),
        }
    }

    /// Records a mutation so the background reclaimer holds off while the store is
    /// active. Cheap (a relaxed increment).
    #[inline]
    fn note_activity(&self) {
        self.activity.fetch_add(1, Ordering::Relaxed);
    }

    /// Starts the background forget worker: receives batches of keys and deletes
    /// their spill/cold rows off the engine actor, in [`FORGET_CHUNK`]-sized
    /// transactions so each commit holds the connection lock only briefly.
    ///
    /// A genuine SQLite delete/checkpoint failure is **not** panicked on (#1366):
    /// under the server's unwind profile a panic would silently kill only this
    /// thread, dropping every queued batch and the WAL-growth backstop while
    /// foreground writes continued. Instead the worker latches the error into
    /// `faults` (observable via [`VarSpillStore::forget_faults`]), retains the
    /// failed batch plus every still-queued batch in the latch (no pending delete
    /// is dropped silently), acks any pending flush barrier so a concurrent
    /// `flush_forgets` cannot hang, and exits — which disconnects the channel, so
    /// later `forget_async*` calls fall back to synchronous deletes and
    /// `flush_forgets` reports `false`. A poisoned connection lock is NOT fatal:
    /// every op is atomic under the lock, so the guard is recovered (matching
    /// `delete_keys` and the reclaim worker) — a poison must never silently stop
    /// a maintenance worker while foreground writes continue.
    // expect_used: thread spawn failure is a fatal OS resource error at store
    // open; there is no recoverable fallback for a missing maintenance thread.
    #[allow(
        clippy::expect_used,
        reason = "thread spawn failure is a fatal OS resource error at store open"
    )]
    fn spawn_forget_worker(
        conn: Arc<Mutex<Connection>>,
        activity: Arc<AtomicU64>,
        faults: Arc<ForgetFaults>,
        #[cfg(test)] injection: Arc<ForgetFaultInjection>,
    ) -> ForgetWorker {
        let (tx, rx) = sync_channel::<ForgetMsg>(forget_queue_cap());
        let handle = std::thread::Builder::new()
            .name("varspill-forget".into())
            .spawn(move || {
                // With wal_autocheckpoint disabled, the WAL only truncates off the
                // hot path. The idle-gated reclaim thread handles the post-load
                // drain, but under *sustained* retirement (never idle) the WAL would
                // grow unbounded, so the worker itself truncates once it has forgotten
                // WAL_CKPT_KEYS keys since the last checkpoint. That bound is large
                // and the checkpoint infrequent, so its fsync barely perturbs raft
                // fsync (unlike the per-1000-page auto-checkpoint it replaces), while
                // still capping WAL disk growth and keeping cold reads fast.
                let ckpt_keys = wal_checkpoint_keys();
                let mut since_ckpt: u64 = 0;
                // Exits when the channel closes (every sender dropped), after
                // draining any batches still queued — so no forget is lost.
                'drain: while let Ok(msg) = rx.recv() {
                    match msg {
                        ForgetMsg::Keys(keys) => {
                            let n = keys.len() as u64;
                            // Test-only seam: force this batch's delete to fail.
                            #[cfg(test)]
                            let delete_result = {
                                injection.wait_at_gate();
                                match injection.take_delete_error() {
                                    Some(err) => Err(err),
                                    None => Self::delete_keys(&conn, &activity, &keys),
                                }
                            };
                            #[cfg(not(test))]
                            let delete_result = Self::delete_keys(&conn, &activity, &keys);
                            // delete_keys bumps `activity` per chunk, so even a long
                            // multi-chunk batch keeps the store non-idle for its whole
                            // duration and the reclaim worker won't start a pass while
                            // deletes are in-flight.
                            if let Err(err) = delete_result {
                                Self::fail_worker(&faults, &rx, &err, keys);
                                break 'drain;
                            }
                            since_ckpt = since_ckpt.saturating_add(n);
                            if since_ckpt >= ckpt_keys {
                                since_ckpt = 0;
                                // A WAL-truncate checkpoint can itself be a long,
                                // fsync-heavy operation; bump activity so the reclaim
                                // worker won't classify the store as idle and start a
                                // vacuum pass during/around this maintenance I/O.
                                activity.fetch_add(1, Ordering::Relaxed);
                                // A poisoned lock means another thread panicked
                                // mid-mutation; the connection is still usable
                                // (every op is atomic under the lock), so recover
                                // the guard rather than kill the worker — matching
                                // `delete_keys` and the reclaim worker, so a poison
                                // never silently stops a maintenance worker while
                                // foreground writes continue (#1366). A genuine
                                // checkpoint I/O failure takes the same observable
                                // fault-latch path as a delete failure (see the
                                // fn-level docs): silently swallowing it would let
                                // the WAL grow without bound under sustained
                                // retirement — the failure this backstop prevents.
                                let checkpoint_result = {
                                    let guard = conn.lock().unwrap_or_else(|e| e.into_inner());
                                    guard.query_row("PRAGMA wal_checkpoint(TRUNCATE)", [], |_| {
                                        Ok(())
                                    })
                                };
                                #[cfg(test)]
                                let checkpoint_result = checkpoint_result.and_then(|()| {
                                    match injection.take_checkpoint_error() {
                                        Some(err) => Err(err),
                                        None => Ok(()),
                                    }
                                });
                                if let Err(err) = checkpoint_result {
                                    // Nothing failed to delete here — the batch was
                                    // already applied — so there are no keys to
                                    // retain beyond what is still queued.
                                    Self::fail_worker(&faults, &rx, &err, Vec::new());
                                    break 'drain;
                                }
                            }
                        }
                        ForgetMsg::Flush(ack) => {
                            let _ = ack.send(());
                        }
                    }
                }
            })
            .expect("spawn varspill-forget thread");
        ForgetWorker {
            tx: Some(tx),
            handle: Some(handle),
        }
    }

    /// The worker's fatal-failure path (#1366): record the fault (which **closes
    /// producer admission** — see [`ForgetFaults::record`]) *before* draining, so
    /// a `forget_async*` racing this shutdown either enqueued before the close
    /// (and is retained by the drain below) or observes the closed flag and takes
    /// the synchronous fallback — no batch is ever accepted into the queue after
    /// the drain has passed it, and none is left stranded. Then drain every
    /// still-queued batch into the latch's retained set (acking any flush barrier
    /// so a concurrent `flush_forgets` never hangs) and return so the thread
    /// exits.
    ///
    /// Exiting drops `rx`, disconnecting the channel — which makes any send that
    /// slipped past the closed flag fail over to the synchronous fallback and
    /// makes `flush_forgets` report `false`. Because `record` runs before the
    /// drain, an observer that sees the latched error knows admission is already
    /// closed and the queue already drained.
    fn fail_worker(
        faults: &ForgetFaults,
        rx: &std::sync::mpsc::Receiver<ForgetMsg>,
        err: &rusqlite::Error,
        failed: Vec<Key>,
    ) {
        // Close producer admission FIRST: after this, `forget_async*` stops
        // enqueueing and falls back to synchronous deletes, so the drain below
        // observes a quiescing queue.
        faults.record(err, failed);
        let mut retained = Vec::new();
        while let Ok(msg) = rx.try_recv() {
            match msg {
                ForgetMsg::Keys(keys) => retained.extend(keys),
                ForgetMsg::Flush(ack) => {
                    let _ = ack.send(());
                }
            }
        }
        faults.retain(retained);
    }

    /// Deletes the spill and cold rows for `keys`, chunked into short transactions
    /// so a large batch never holds the connection lock for long (blocking the
    /// engine actor's own spill/rehydrate access). Bumps `activity` once per chunk
    /// so the reclaim worker sees the store as busy for the entire (potentially
    /// long) sweep, not just at its start.
    ///
    /// Returns the typed [`rusqlite::Error`] on any SQLite failure (#1366): a
    /// panicking `expect` here would, under the server's unwind profile, silently
    /// kill only the calling (worker) thread and drop the deletes this path exists
    /// to apply. Each chunk is one atomic transaction, so an `Err` leaves that
    /// chunk (and all later chunks) unapplied and every earlier chunk durably
    /// deleted — the caller decides how to surface and retain the unapplied keys
    /// (the forget worker latches them; the synchronous [`forget`](Self::forget)
    /// path returns the error to its caller).
    fn delete_keys(
        conn: &Mutex<Connection>,
        activity: &AtomicU64,
        keys: &[Key],
    ) -> rusqlite::Result<()> {
        for chunk in keys.chunks(FORGET_CHUNK) {
            activity.fetch_add(1, Ordering::Relaxed);
            // A poisoned lock means another thread panicked mid-mutation; the
            // connection itself is still usable (every op is atomic under the
            // lock), so recover the guard rather than abort the forget path.
            let mut guard = conn.lock().unwrap_or_else(|e| e.into_inner());
            // Propagate any SQLite failure to the caller (see the fn-level docs):
            // silently swallowing a transaction/prepare/execute/commit error would
            // drop deletes mid-batch and reintroduce the orphan cold rows (and
            // unbounded file growth) this path exists to prevent.
            let tx = guard.transaction()?;
            {
                let mut del_spill = tx.prepare_cached("DELETE FROM spill WHERE key = ?1")?;
                let mut del_cold = tx.prepare_cached("DELETE FROM cold WHERE key = ?1")?;
                for &key in chunk {
                    del_spill.execute(params![key as i64])?;
                    del_cold.execute(params![key as i64])?;
                }
            }
            tx.commit()?;
        }
        Ok(())
    }

    /// Persists `vars` under `key`, replacing any prior payload.
    pub fn put(&self, key: Key, vars: &HashMap<String, Value>) -> rusqlite::Result<()> {
        let json = serde_json::to_string(vars)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "INSERT OR REPLACE INTO spill (key, vars) VALUES (?1, ?2)",
            params![key as i64, json],
        )?;
        drop(conn);
        self.note_activity();
        Ok(())
    }

    /// Returns the payload for `key` **without removing it**, or `None` if absent.
    /// Used to fold spilled payloads into a self-contained snapshot (#1331): the
    /// live instance stays spilled, so its row must survive the read.
    pub fn get(&self, key: Key) -> Option<HashMap<String, Value>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let json = Self::select(&conn, "SELECT vars FROM spill WHERE key = ?1", key)?;
        drop(conn);
        serde_json::from_str(&json).ok()
    }

    /// Removes and returns the payload for `key`, or `None` if absent. A spilled
    /// instance is rehydrated exactly once (on activation), so taking the row on
    /// read keeps the store bounded to the still-cold backlog.
    pub fn take(&self, key: Key) -> Option<HashMap<String, Value>> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let json = Self::select(&conn, "SELECT vars FROM spill WHERE key = ?1", key)?;
        let _ = conn.execute("DELETE FROM spill WHERE key = ?1", params![key as i64]);
        drop(conn);
        self.note_activity();
        serde_json::from_str(&json).ok()
    }

    /// The single-column text row for `key` under `sql`, or `None` if absent.
    fn select(conn: &Connection, sql: &str, key: Key) -> Option<String> {
        conn.query_row(sql, params![key as i64], |r| r.get(0))
            .optional()
            .ok()?
    }

    /// Persists a whole-instance cold [`InstanceSnapshot`] under `key`, replacing
    /// any prior snapshot. Shares the connection (and thus the WAL) with the
    /// variable spill above, so the two tiers live in one file and one durability
    /// story — the reason cold spill reuses this store rather than a second DB.
    pub fn put_cold(&self, key: Key, snapshot: &InstanceSnapshot) -> rusqlite::Result<()> {
        let json = serde_json::to_string(snapshot)
            .map_err(|e| rusqlite::Error::ToSqlConversionFailure(Box::new(e)))?;
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        conn.execute(
            "INSERT OR REPLACE INTO cold (key, snapshot) VALUES (?1, ?2)",
            params![key as i64, json],
        )?;
        drop(conn);
        self.note_activity();
        Ok(())
    }

    /// Returns the cold snapshot for `key` **without removing it**, or `None` if
    /// absent. Used to fold a still-cold instance into a self-contained snapshot
    /// (#1331) while it stays off-heap.
    pub fn get_cold(&self, key: Key) -> Option<InstanceSnapshot> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let json = Self::select(&conn, "SELECT snapshot FROM cold WHERE key = ?1", key)?;
        drop(conn);
        serde_json::from_str(&json).ok()
    }

    /// Removes and returns the cold snapshot for `key`, or `None` if absent.
    /// Destructive on read (like [`take`](VarSpillStore::take)): rehydrating an
    /// instance takes its snapshot back out, so the cold table holds only the
    /// still-dormant backlog.
    pub fn take_cold(&self, key: Key) -> Option<InstanceSnapshot> {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        let json = Self::select(&conn, "SELECT snapshot FROM cold WHERE key = ?1", key)?;
        let _ = conn.execute("DELETE FROM cold WHERE key = ?1", params![key as i64]);
        drop(conn);
        self.note_activity();
        serde_json::from_str(&json).ok()
    }

    /// Drops any spilled variable and cold-snapshot rows for `keys`, in one
    /// transaction. Called when instances reach a terminal state and are evicted
    /// from hot state: their spilled payloads are now dead and would otherwise
    /// accumulate as orphan rows (the store is destructive only on *rehydration*,
    /// and a terminal instance is never rehydrated). Absent keys are no-ops, so
    /// this is safe to call for every evicted instance whether or not it spilled.
    ///
    /// The freed pages land on the freelist; returning them to the OS is left to the
    /// background reclaim thread, which drains only while the store is quiescent — so
    /// this eviction path stays free of the fsync-heavy vacuum/checkpoint that would
    /// otherwise contend with the engine's raft-log fsync under load.
    ///
    /// On a SQLite failure the error is latched into [`Self::forget_faults`] (so the
    /// loss is observable and never silent), the unapplied `keys` are retained in
    /// the same latch (so the pending-delete record survives the failure and no
    /// orphan rows are silently dropped), **and** the typed error is returned to
    /// the caller (#1366) — never panicked: this path runs on the replica actor
    /// thread, where under the server's unwind profile a panic would take the
    /// whole actor down.
    pub fn forget(&self, keys: &[Key]) -> rusqlite::Result<()> {
        if keys.is_empty() {
            return Ok(());
        }
        // delete_keys bumps activity per chunk, so no separate note_activity is needed.
        Self::delete_keys(&self.conn, &self.activity, keys).inspect_err(|err| {
            // Retain the unapplied keys alongside the fault: the caller owns the
            // batch but (on the fire-and-forget retirement/eviction paths) drops
            // it on error, so recording only the error text would lose the
            // pending-delete record and leave orphan rows (#1366).
            self.faults.record(err, keys.to_vec());
        })
    }

    /// Like [`forget`](Self::forget), but hands the deletes to the background
    /// [`ForgetWorker`] instead of running them on the caller's thread. Used by the
    /// follower retirement paths ([`crate::journal::Journal::retire_below`] /
    /// `retire_instances`), which run on the single-writer replica actor and would
    /// otherwise stall live raft replication with a large synchronous DELETE
    /// transaction every retirement tick. Falls back to a synchronous
    /// [`forget`](Self::forget) for in-memory stores (no worker) so tests observe
    /// the delete immediately; keys are unique and never reused, so a queued forget
    /// can never clobber a later re-spill of the same key.
    pub fn forget_async(&self, keys: &[Key]) {
        if keys.is_empty() {
            return;
        }
        self.forget_async_owned(keys.to_vec());
    }

    /// Owned variant of [`forget_async`](Self::forget_async) that moves the batch
    /// into the worker queue without cloning. Callers that already own a `Vec<Key>`
    /// (notably [`crate::journal::Journal::retire_below`]'s reaped set, up to 100k
    /// keys per tick) should use this so the single-writer replica actor doesn't pay
    /// a large copy just to hand the deletes off.
    pub fn forget_async_owned(&self, keys: Vec<Key>) {
        if keys.is_empty() {
            return;
        }
        // Producer admission is coordinated with the worker's fault transition
        // (#1366): once the worker records a fault it closes admission BEFORE
        // draining the queue, so a batch is either enqueued before the close
        // (and retained by the worker's drain) or sees `is_closed` here and
        // takes the synchronous fallback — never accepted into a queue the
        // exiting worker's drain already passed, and never stranded.
        //
        // Non-blocking handoff. If the bounded queue is full (slow consumer) or
        // the worker has gone away (faulted — see `forget_faults`), recover the
        // batch and delete synchronously so a producer > consumer imbalance
        // applies backpressure here instead of growing the queue's memory
        // without bound. A synchronous fallback failure is latched + retained
        // by `forget`; this fire-and-forget retirement path surfaces it through
        // `forget_faults`.
        let tx = self.forget.as_ref().and_then(|w| w.tx.as_ref());
        match tx {
            Some(tx) if !self.faults.is_closed() => {
                // Only `Keys` is ever sent on this path, so a Full/Disconnected
                // error always hands the same batch back for the synchronous
                // fallback.
                if let Err(TrySendError::Full(ForgetMsg::Keys(k)))
                | Err(TrySendError::Disconnected(ForgetMsg::Keys(k))) =
                    tx.try_send(ForgetMsg::Keys(keys))
                {
                    let _ = self.forget(&k);
                }
            }
            _ => {
                let _ = self.forget(&keys);
            }
        }
    }

    /// Blocks until every batch queued on the background forget worker before this
    /// call has been applied. A no-op for in-memory stores (forgets run inline).
    /// Used by tests and graceful shutdown to make the async deletes observable.
    ///
    /// Returns `false` when the worker is gone (channel disconnected) instead of
    /// panicking: a false "flushed" signal would mask lost or still-pending
    /// deletes, so callers observe the failure explicitly. A worker that stopped
    /// on a fatal SQLite error (#1366) acks any already-queued barrier before
    /// exiting (so this never hangs), then disconnects the channel — the `false`
    /// comes from the failed send, and [`Self::forget_faults`] carries the error
    /// and the retained (unapplied) keys.
    pub fn flush_forgets(&self) -> bool {
        let Some(tx) = self.forget.as_ref().and_then(|w| w.tx.as_ref()) else {
            return true;
        };
        let (ack_tx, ack_rx) = channel();
        // A live store keeps its worker running until `ForgetWorker::drop` closes
        // the channel, so a failed send or ack-recv here means the worker has
        // exited (faulted). Surface that (as `false`) rather than returning a
        // false "flushed" signal to tests / graceful shutdown (which would mask
        // lost or still-pending deletes — the orphan-row leak this path guards
        // against).
        if tx.send(ForgetMsg::Flush(ack_tx)).is_err() {
            return false;
        }
        ack_rx.recv().is_ok()
    }

    /// The latched forget/delete failure state (#1366): `Some` once the
    /// background forget worker (or a synchronous [`forget`](Self::forget)) hit a
    /// fatal SQLite error, carrying the first error and the keys whose deletes
    /// were retained (not applied) when the worker stopped. Empty while the store
    /// is healthy.
    pub fn forget_faults(&self) -> &ForgetFaults {
        &self.faults
    }

    /// The store's on-disk size as `(file_bytes, live_bytes)` (see
    /// [`crate::sqlite_space::page_stats`]): `file_bytes` is the whole allocated
    /// file (freelist included), `live_bytes` the pages holding actual data. With
    /// incremental auto-vacuum + background reclaim, `file_bytes` settles toward the
    /// live cold backlog once a spike drains, rather than plateauing at the peak.
    pub fn db_page_stats(&self) -> (u64, u64) {
        let conn = self.conn.lock().unwrap_or_else(|e| e.into_inner());
        page_stats(&conn)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn vars(payload: &str) -> HashMap<String, Value> {
        let mut m = HashMap::new();
        m.insert("data".to_string(), Value::Str(payload.to_string()));
        m
    }

    #[test]
    fn round_trips_a_payload() {
        let store = VarSpillStore::open(None).unwrap();
        store.put(7, &vars("hello")).unwrap();
        let got = store.take(7).expect("payload present");
        assert_eq!(got.get("data"), Some(&Value::Str("hello".to_string())));
    }

    /// Epic #1357 failure-injection guard: a poisoned connection mutex must be
    /// RECOVERED (the guard is taken from the poison) on every public path —
    /// never a panic. Poisoning means some thread panicked mid-mutation; the
    /// store stays available because every operation here is atomic under the
    /// lock (a panic between operations leaves the connection usable).
    #[test]
    fn poisoned_connection_lock_is_recovered_not_panicked() {
        let store = VarSpillStore::open(None).unwrap();
        store.put(1, &vars("before-poison")).unwrap();

        // Poison the mutex: panic while holding the guard.
        let conn = Arc::clone(&store.conn);
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = conn.lock().unwrap();
            panic!("deliberate poison for the recovery test");
        }));
        std::panic::set_hook(prev);
        assert!(poisoned.is_err(), "the poisoning panic must propagate");
        assert!(
            store.conn.lock().is_err(),
            "the mutex is now poisoned (test precondition)"
        );

        // Every public path must still work — no panics.
        store.put(2, &vars("after-poison")).unwrap();
        assert_eq!(store.get(1), Some(vars("before-poison")));
        assert_eq!(store.take(2), Some(vars("after-poison")));
        let snapshot = cold_snapshot(3, "cold");
        store.put_cold(3, &snapshot).unwrap();
        assert_eq!(store.get_cold(3), Some(snapshot.clone()));
        assert_eq!(store.take_cold(3), Some(snapshot));
        let (file_bytes, live_bytes) = store.db_page_stats();
        assert!(
            live_bytes > 0,
            "stats still read: ({file_bytes}, {live_bytes})"
        );
        store.forget(&[1]).unwrap();
        assert!(store.get(1).is_none(), "forget still deletes");
    }

    /// Epic #1357 / #1366 failure-injection guard: on a **file-backed** store the
    /// background forget worker holds the connection lock too, and it must RECOVER
    /// a poisoned lock (take the guard from the poison) rather than panic or exit
    /// silently. Under `panic = "unwind"` a worker panic only kills the worker
    /// thread, so a poison that stopped the worker would silently drop queued
    /// retirement deletes (and the WAL-growth checkpoint backstop) while foreground
    /// writes kept running — the failure mode this test guards against. In-memory
    /// stores forget inline and spawn no worker, so this needs a real file.
    #[test]
    fn poisoned_connection_lock_is_recovered_in_file_backed_forget_worker() {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-varspill-poison-worker-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("var-spill.sqlite");
        let store = VarSpillStore::open(Some(&path)).unwrap();
        store.put(1, &vars("a")).unwrap();
        store.put(2, &vars("b")).unwrap();

        // Poison the shared connection mutex: panic while holding the guard.
        let conn = Arc::clone(&store.conn);
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let poisoned = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            let _guard = conn.lock().unwrap();
            panic!("deliberate poison for the recovery test");
        }));
        std::panic::set_hook(prev);
        assert!(poisoned.is_err(), "the poisoning panic must propagate");
        assert!(
            store.conn.lock().is_err(),
            "the mutex is now poisoned (test precondition)"
        );

        // The background forget worker must recover the poison and still apply the
        // delete — not exit silently or panic. `flush_forgets` returns `false` iff
        // the worker has gone away, so a `true` here proves it survived the poison
        // and drained the queued batch.
        store.forget_async(&[1]);
        assert!(
            store.flush_forgets(),
            "forget worker survived the poison and drained its queue"
        );
        assert!(
            store.get(1).is_none(),
            "the async forget still deleted key 1 through the recovered lock"
        );

        // Foreground writes keep working through the recovered (still-poisoned)
        // lock, so the store stays available end to end.
        store.put(3, &vars("c")).unwrap();
        assert_eq!(store.get(2), Some(vars("b")));
        assert_eq!(store.get(3), Some(vars("c")));

        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #1366 failure-injection guard (delete path): a genuine SQLite error in the
    /// forget worker's delete must NOT panic the worker (under `panic = "unwind"`
    /// that would silently kill only the worker thread, dropping every queued
    /// batch). The worker latches the error into `forget_faults`, retains the
    /// failed batch plus every still-queued batch, acks a queued flush barrier,
    /// and exits — disconnecting the channel so `flush_forgets` reports `false`
    /// and later `forget_async*` calls fall back to synchronous deletes.
    ///
    /// Deterministic: the failure is gated (see
    /// [`ForgetFaultInjection::arm_gated_delete_failure`]) so the second batch and
    /// the flush barrier are queued *while the worker is parked at the failure
    /// point*, not by racing the scheduler.
    #[test]
    fn forget_worker_delete_error_latches_fault_and_retains_pending_keys() {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-varspill-delerr-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("var-spill.sqlite");
        let store = VarSpillStore::open(Some(&path)).unwrap();
        store.put(1, &vars("a")).unwrap();
        store.put(2, &vars("b")).unwrap();
        store.put(3, &vars("c")).unwrap();

        // Break deletes: dropping the `spill` table makes the worker's
        // `DELETE FROM spill` fail with a real SQLite error on the next batch.
        {
            let conn = store.conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute("DROP TABLE spill", []).unwrap();
        }

        // Arm the gated failure, then queue the first batch. The worker receives
        // it and parks at the gate just before the (injected) failing delete.
        let (parked, release) = store.fault_injection.arm_gated_delete_failure();
        store.forget_async(&[1]);
        parked
            .recv()
            .expect("the worker parks at the gate before its failing delete");

        // While the worker is parked at the failure point, queue the second
        // batch and the flush barrier behind it — deterministically, with no
        // scheduling race. Then release the worker into the failure.
        store.forget_async(&[2]);
        let (ack_tx, ack_rx) = channel();
        {
            let tx = store.forget.as_ref().and_then(|w| w.tx.as_ref());
            tx.expect("worker alive")
                .send(ForgetMsg::Flush(ack_tx))
                .expect("barrier enqueued while the worker is parked");
        }
        release.send(()).expect("release the gated worker");

        // The worker fails batch [1], closes admission, retains [1]+[2], acks
        // the already-queued barrier (so this recv cannot hang), and exits —
        // disconnecting the channel.
        ack_rx
            .recv()
            .expect("the barrier queued behind the batches is acked by the failing worker");
        // Wait for the fault to be latched. `record` closes admission and sets
        // the error BEFORE the drain, so once the error is visible the worker is
        // past the drain and (bar a thread-deschedule) has exited; a post-fault
        // flush then reports `false` (the send fails on the disconnected
        // channel) rather than racing a not-yet-exited worker.
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while store.forget_faults().error().is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "the delete failure is latched observably (timed out waiting)"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !store.flush_forgets(),
            "once the faulted worker has exited, flush_forgets() == false"
        );
        let mut retained = store.forget_faults().retained_keys();
        retained.sort_unstable();
        assert_eq!(
            retained,
            vec![1, 2],
            "the failed batch and the still-queued batch are both retained, not dropped"
        );

        // No silent loss: with the table restored, the synchronous fallback path
        // (the worker is gone, so forget_async falls back) still applies deletes.
        // Reinsert key 3 first — `DROP TABLE spill` already removed it, so
        // without a reinsert the `get(3).is_none()` assertion below would pass
        // vacuously even if the fallback did nothing.
        {
            let conn = store.conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute(
                "CREATE TABLE IF NOT EXISTS spill (key INTEGER PRIMARY KEY, vars TEXT NOT NULL)",
                [],
            )
            .unwrap();
        }
        store.put(3, &vars("c")).unwrap();
        assert_eq!(store.get(3), Some(vars("c")), "key 3 reinserted");
        store.forget_async(&[3]);
        assert!(
            store.get(3).is_none(),
            "post-fault forget_async falls back to a synchronous delete"
        );

        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #1366 failure-injection guard (checkpoint path): a failing
    /// `wal_checkpoint(TRUNCATE)` takes the same observable fault-latch path as a
    /// delete error — the worker latches the fault and stops rather than
    /// panicking (silently killing the thread under unwind) or swallowing the
    /// error (letting the WAL grow without bound). Uses the cfg(test) injection
    /// seam: a real checkpoint error cannot be forced deterministically on a
    /// healthy connection, and the env knob is process-global (races parallel
    /// tests).
    #[test]
    fn forget_worker_checkpoint_error_latches_fault_and_stops() {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-varspill-ckpterr-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("var-spill.sqlite");
        let store = VarSpillStore::open(Some(&path)).unwrap();

        // Arm the seam: the worker's next checkpoint fails after the batch's
        // deletes have been applied (so there is nothing to retain from it).
        store
            .fault_injection
            .fail_next_checkpoint
            .store(true, Ordering::Relaxed);
        // Queue at least WAL_CKPT_KEYS keys so the checkpoint fires on this batch.
        let n = wal_checkpoint_keys();
        store.forget_async_owned((0..n).collect());

        // Wait for the fault to be recorded. `record` closes admission and sets
        // the error BEFORE the drain, so once the error is visible the worker is
        // past the drain and (bar a thread-deschedule) has exited; a post-fault
        // flush then reports `false` on the disconnected channel — no scheduling
        // race between "worker exiting" and "flush observing the exit".
        let deadline = std::time::Instant::now() + Duration::from_secs(10);
        while store.forget_faults().error().is_none() {
            assert!(
                std::time::Instant::now() < deadline,
                "the checkpoint failure is latched observably (timed out waiting)"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
        assert!(
            !store.flush_forgets(),
            "once the checkpoint-faulted worker has exited, flush_forgets() == false"
        );

        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// #1366: the synchronous `forget` path (replica actor thread) returns the
    /// typed error and latches the same fault — it must never panic, because
    /// under `panic = "unwind"` a panic there would take the whole actor down.
    /// The unapplied keys are retained in the latch so the pending-delete record
    /// survives the failure (no orphan rows dropped silently).
    #[test]
    fn sync_forget_returns_typed_error_and_latches_fault() {
        let store = VarSpillStore::open(None).unwrap();
        store.put(1, &vars("a")).unwrap();
        {
            let conn = store.conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute("DROP TABLE spill", []).unwrap();
        }

        let err = store
            .forget(&[1])
            .expect_err("a broken delete surfaces as a typed error, not a panic");
        assert!(
            err.to_string().contains("spill"),
            "the typed error names the failed statement's table: {err}"
        );
        assert!(
            store.forget_faults().error().is_some(),
            "the synchronous failure is latched into the same observable fault"
        );
        assert_eq!(
            store.forget_faults().retained_keys(),
            vec![1],
            "the unapplied keys are retained, not dropped"
        );
    }

    /// #1366 regression guard: a failing **synchronous fallback** in
    /// `forget_async_owned` (queue full or worker gone) must retain the batch in
    /// `forget_faults` — the fire-and-forget retirement path drops the owned
    /// batch on error, so without retention the pending-delete record is lost
    /// and the rows orphan silently.
    #[test]
    fn failing_sync_fallback_retains_the_batch() {
        // In-memory store: no forget worker, so `forget_async` always runs the
        // synchronous fallback inline.
        let store = VarSpillStore::open(None).unwrap();
        store.put(7, &vars("x")).unwrap();
        {
            let conn = store.conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute("DROP TABLE spill", []).unwrap();
        }

        store.forget_async(&[7]);
        assert!(
            store.forget_faults().error().is_some(),
            "the fallback failure is latched observably"
        );
        assert_eq!(
            store.forget_faults().retained_keys(),
            vec![7],
            "the fallback's failed batch is retained, not dropped"
        );
    }

    /// #1366 regression guard for the fault-transition shutdown window: a batch
    /// enqueued concurrently with the worker's failure handling must NEVER be
    /// accepted into the queue after the worker's drain has already passed it
    /// (which would leave it neither retained nor deleted). Admission is closed
    /// *before* the drain, so a racing producer either lands in the queue before
    /// the close (and is retained by the drain) or sees the closed flag and
    /// takes the synchronous fallback. Either way the key is accounted for:
    /// deleted, or present in `retained_keys`.
    #[test]
    fn forget_async_during_fault_transition_is_never_lost() {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-varspill-race-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("var-spill.sqlite");
        let store = VarSpillStore::open(Some(&path)).unwrap();
        store.put(1, &vars("a")).unwrap();
        store.put(9, &vars("z")).unwrap();

        // Break deletes so the worker's next delete fails.
        {
            let conn = store.conn.lock().unwrap_or_else(|e| e.into_inner());
            conn.execute("DROP TABLE spill", []).unwrap();
        }

        // Arm the gated failure and queue the failing batch; the worker parks at
        // the gate just before the delete.
        let (parked, release) = store.fault_injection.arm_gated_delete_failure();
        store.forget_async(&[1]);
        parked.recv().expect("worker parked at the gate");

        // While the worker is parked, hammer `forget_async` from another thread
        // so some sends race the fault transition (the close + drain + exit).
        // `std::thread::scope` joins the racer before returning.
        std::thread::scope(|s| {
            s.spawn(|| {
                for _ in 0..50 {
                    store.forget_async(&[9]);
                }
            });
            // Release the worker into the failure while the racer is mid-stream.
            release.send(()).expect("release the gated worker");
        });

        // After the dust settles: the worker has faulted and exited. Key 9 was
        // either deleted (a fallback that ran before/without the fault, or an
        // early enqueue the worker applied — impossible here since deletes are
        // broken) or retained. Because deletes are broken, EVERY key-9 batch
        // must be retained: none may be silently dropped in the shutdown window.
        let retained = store.forget_faults().retained_keys();
        assert!(
            store.forget_faults().error().is_some(),
            "the fault is latched"
        );
        assert!(
            retained.contains(&1),
            "the failed batch is retained: {retained:?}"
        );
        assert!(
            retained.contains(&9),
            "a racing batch enqueued during the fault transition is retained, not lost: {retained:?}"
        );

        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    // #1331: the store's rows must survive a process restart (re-open), because
    // a snapshot written before self-contained snapshots may still reference
    // them; and `get`/`get_cold` must read without consuming.
    #[test]
    fn rows_survive_reopen_and_get_is_non_destructive() {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-varspill-reopen-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("var-spill.sqlite");
        let mut vars = HashMap::new();
        vars.insert("k".to_string(), Value::Int(7));
        {
            let store = VarSpillStore::open(Some(&path)).unwrap();
            store.put(1, &vars).unwrap();
        }
        let store = VarSpillStore::open(Some(&path)).unwrap();
        assert_eq!(store.get(1), Some(vars.clone()), "row survives re-open");
        assert_eq!(store.get(1), Some(vars.clone()), "get does not consume");
        assert_eq!(store.take(1), Some(vars), "take still returns it");
        assert!(store.get(1).is_none(), "take consumed it");
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn take_is_destructive_and_absent_is_none() {
        let store = VarSpillStore::open(None).unwrap();
        store.put(1, &vars("x")).unwrap();
        assert!(store.take(1).is_some());
        assert!(store.take(1).is_none(), "second take sees nothing");
        assert!(store.take(999).is_none(), "absent key is None");
    }

    fn cold_snapshot(key: Key, payload: &str) -> InstanceSnapshot {
        use nanobpmn_engine_core::{ProcessInstance, ProcessInstanceState};

        InstanceSnapshot {
            instance: ProcessInstance {
                key,
                process_id: "order".to_string(),
                process_definition_key: 0,
                state: ProcessInstanceState::Active,
                created_at: 1,
                tags: Vec::new(),
                business_id: None,
                parent_process_instance_key: None,
                parent_element_instance_key: None,
                suspended_at: None,
                active: HashMap::new(),
                scopes: HashMap::new(),
                variables: Arc::new(vars(payload)),
                join_counts: HashMap::new(),
                join_flow_arrivals: HashMap::new(),
                join_instances: HashMap::new(),
                incidents: Vec::new(),
                variables_spilled: false,
                multi_instances: HashMap::new(),
                adhoc_instances: HashMap::new(),
                scope_parents: HashMap::new(),
                scope_variables: HashMap::new(),
                compensable: Vec::new(),
                compensation_waits: HashMap::new(),
                agent_instances: HashMap::new(),
                agent_history: HashMap::new(),
            },
            jobs: Vec::new(),
            timers: Vec::new(),
            message_subscriptions: Vec::new(),
            signal_subscriptions: Vec::new(),
            conditional_subscriptions: Vec::new(),
            user_tasks: Vec::new(),
            incidents: Vec::new(),
        }
    }

    #[test]
    fn cold_snapshot_round_trips() {
        let store = VarSpillStore::open(None).unwrap();
        let snapshot = cold_snapshot(42, "payload");
        store.put_cold(42, &snapshot).unwrap();
        let got = store.take_cold(42).expect("snapshot present");
        assert_eq!(got, snapshot);
        assert!(store.take_cold(42).is_none(), "take_cold is destructive");
        assert!(store.take_cold(7).is_none(), "absent key is None");
    }

    // #1331: the new cold-read contract — a cold row must survive a process
    // restart (re-open) and repeated non-destructive `get_cold` calls, exactly as
    // the variable-spill reopen test above covers `get`. Without this guard the
    // file-backed cold tier has no proof a still-cold instance is recoverable
    // after a reboot.
    #[test]
    fn cold_snapshot_survives_reopen_and_repeated_get_cold() {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-varspill-cold-reopen-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        let path = dir.join("var-spill.sqlite");
        let snapshot = cold_snapshot(9, "cold-payload");
        {
            let store = VarSpillStore::open(Some(&path)).unwrap();
            store.put_cold(9, &snapshot).unwrap();
        }
        let store = VarSpillStore::open(Some(&path)).unwrap();
        assert_eq!(
            store.get_cold(9),
            Some(snapshot.clone()),
            "cold row survives re-open"
        );
        assert_eq!(
            store.get_cold(9),
            Some(snapshot.clone()),
            "get_cold does not consume the row"
        );
        assert_eq!(
            store.take_cold(9),
            Some(snapshot),
            "take_cold still returns it"
        );
        assert!(store.get_cold(9).is_none(), "take_cold consumed it");
        drop(store);
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn forget_drops_spill_and_cold_rows_and_ignores_absent_keys() {
        let store = VarSpillStore::open(None).unwrap();
        store.put(1, &vars("a")).unwrap();
        store.put(2, &vars("b")).unwrap();
        store.put(3, &vars("c")).unwrap();

        // Forgetting terminal instances drops their rows; an absent key (99) is a
        // no-op, and an untouched key (3) survives.
        store.forget(&[1, 2, 99]).unwrap();

        assert!(store.take(1).is_none(), "forgotten spill row gone");
        assert!(store.take(2).is_none(), "forgotten spill row gone");
        assert!(store.take(3).is_some(), "untouched spill row survives");

        // forget also clears the cold tier for the same key.
        store.put(4, &vars("d")).unwrap();
        store.forget(&[4]).unwrap();
        assert!(store.take(4).is_none(), "forget clears spill tier for key");

        // Empty slice is a cheap no-op.
        store.forget(&[]).unwrap();
    }

    #[test]
    fn forget_then_idle_reclaims_file_space_to_the_os() {
        // A file-backed store so we can observe the on-disk high-water shrink. The
        // reclaim gate is 1 byte (any freelist qualifies) and the idle window is
        // short so the background drainer fires quickly once we stop mutating.
        let dir = std::env::temp_dir();
        let path = dir.join(format!("varspill-reclaim-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let store = VarSpillStore::open_with_params(Some(&path), 1, 100).unwrap();

        // Spill a batch large enough to grow the file well past its empty size.
        let big = "x".repeat(8 * 1024);
        let keys: Vec<Key> = (0..4000).collect();
        for &k in &keys {
            store.put(k, &vars(&big)).unwrap();
        }
        let (file_peak, live_peak) = store.db_page_stats();
        assert!(live_peak > 0 && file_peak > 0);

        // Terminal eviction frees the rows onto the freelist but does NOT reclaim
        // inline any more — the file still holds the high-water right after forget.
        store.forget(&keys).unwrap();
        let (file_right_after, live_after) = store.db_page_stats();
        assert!(
            live_after < live_peak / 4,
            "live bytes should collapse after forgetting the batch (peak={live_peak}, after={live_after})"
        );

        // Once the store goes quiet, the background reclaimer drains the freelist and
        // the file shrinks back toward the live set. Poll until it does (bounded).
        let mut file_after = file_right_after;
        for _ in 0..100 {
            std::thread::sleep(std::time::Duration::from_millis(50));
            file_after = store.db_page_stats().0;
            if file_after < file_peak / 2 {
                break;
            }
        }
        assert!(
            file_after < file_peak / 2,
            "background reclaim should shrink the file toward the live set once idle (peak={file_peak}, after={file_after})"
        );

        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn file_backed_store_disables_wal_autocheckpoint() {
        // Regression for the #287 50KB throughput regression: the forget worker's
        // cold-row DELETEs must never trigger SQLite's implicit 1000-page
        // wal_checkpoint, whose fsync (on the shared disk) inflates live raft-log
        // fsync ~7x. The store pins wal_autocheckpoint=0 so the WAL only truncates
        // off the hot path (idle reclaim + the size-gated forget-worker backstop).
        let dir = std::env::temp_dir();
        let path = dir.join(format!("varspill-noautockpt-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let store = VarSpillStore::open_with_params(Some(&path), 1, 100).unwrap();
        let autockpt: i64 = store
            .conn
            .lock()
            .unwrap()
            .query_row("PRAGMA wal_autocheckpoint", [], |r| r.get(0))
            .unwrap();
        assert_eq!(
            autockpt, 0,
            "auto-checkpoint must be disabled on the hot path"
        );

        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn forget_async_deletes_rows_off_thread_and_flush_makes_them_observable() {
        // A file-backed store spawns the background forget worker; `forget_async`
        // hands the deletes to it, and `flush_forgets` blocks until they land.
        let dir = std::env::temp_dir();
        let path = dir.join(format!("varspill-forget-{}.sqlite", std::process::id()));
        let _ = std::fs::remove_file(&path);

        let store = VarSpillStore::open_with_params(Some(&path), 0, 2000).unwrap();
        store.put(1, &vars("a")).unwrap();
        store.put(2, &vars("b")).unwrap();
        store.put(3, &vars("c")).unwrap();

        // Enqueue the forget; the actual DELETEs run on the worker thread.
        store.forget_async(&[1, 2]);
        // A flush barrier acks only after every batch queued before it is applied.
        assert!(store.flush_forgets(), "forget worker flushed");

        assert!(
            store.take(1).is_none(),
            "async-forgotten row gone after flush"
        );
        assert!(
            store.take(2).is_none(),
            "async-forgotten row gone after flush"
        );
        assert!(store.take(3).is_some(), "untouched row survives");

        // Empty slice is a cheap no-op (no message queued).
        store.forget_async(&[]);
        assert!(store.flush_forgets(), "forget worker flushed");

        drop(store);
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn forget_async_falls_back_to_sync_for_in_memory_stores() {
        // In-memory stores have no worker; `forget_async` must delete inline so
        // callers/tests observe the effect immediately, with no flush needed.
        let store = VarSpillStore::open(None).unwrap();
        store.put(5, &vars("x")).unwrap();
        store.forget_async(&[5]);
        assert!(
            store.take(5).is_none(),
            "in-memory forget_async is synchronous"
        );
        // flush is a no-op when there is no worker.
        assert!(store.flush_forgets(), "forget worker flushed");
    }
}
