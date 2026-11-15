//! # Deepthi — the single-writer engine actor
//!
//! A single-writer command actor that owns the durable [`Journal`].
//!
//! Named for **Deepthi Akkoorath**, whose work on concurrency and distributed
//! consensus — including a randomized simulation framework for reliably
//! reproducing concurrent-interaction bugs in the core consensus algorithm — is
//! the inspiration for this subsystem. The actor model here descends from Erlang:
//! a single owner, a serial mailbox, and isolation instead of shared-memory
//! locks. Artists sign their work.
//!
//! The engine is a single writer, so every mutation must be serialized. The
//! previous design wrapped the [`Journal`] in a `std::sync::RwLock` shared across
//! all request tasks. That is an anti-pattern under an async runtime: a blocking
//! `std` lock contended by many tasks *parks the tokio worker thread* holding it
//! instead of yielding, so a burst of concurrent commands stalls the whole
//! runtime — measured here as a 240× collapse in per-request latency from only a
//! handful of concurrent producers.
//!
//! Instead, the [`Journal`] lives on one dedicated thread and commands reach it
//! through an `mpsc` channel; callers `await` a one-shot reply. This mirrors
//! Zeebe's per-partition `StreamProcessor` actor: a single owner, a serial
//! command queue, and tokio workers that only ever park on an async `await`
//! (never on a blocking lock). Serde-heavy request decode / response encode is
//! done by the caller *around* [`DeepthiHandle::with`], so the 50 KB variable
//! payloads are converted in parallel across cores rather than serially on the
//! engine thread.

use std::collections::VecDeque;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, Condvar, Mutex, OnceLock};
use std::thread;
use std::time::Instant;

use tokio::sync::oneshot;

use crate::backpressure::AdaptiveController;
use crate::journal::Journal;

/// A unit of work executed on the engine thread against the owned [`Journal`].
type Job = Box<dyn FnOnce(&mut Journal) + Send>;

/// Monotonic milliseconds since the first call anywhere in the process. Used to
/// stamp when the engine thread picked up its current job so a sampler can read
/// the in-progress job's elapsed time (the wedge detector) without a wallclock.
/// Never returns `0` (offset by 1) so a freshly-stamped `job_start_mono_ms` can't
/// collide with the `0 == idle` sentinel on the very first call.
fn mono_ms() -> u64 {
    static EPOCH: OnceLock<Instant> = OnceLock::new();
    EPOCH.get_or_init(Instant::now).elapsed().as_millis() as u64 + 1
}

/// Liveness/heartbeat telemetry for one partition's engine actor, sampled ~1 Hz
/// by the metrics monitor and published as `nanobpm_actor_*` gauges. The whole
/// point is to disambiguate the sustained-load completion-freeze: a single
/// writer that stops committing is either **dead** (thread exited/panicked —
/// `alive=0`, `jobs` frozen), **wedged** inside one job (`current_job_ms` climbs
/// without bound while `jobs` is frozen and `hi_depth` piles up), or **idle**
/// (everything flat, `job_start_mono_ms==0`, depths 0 — the stall is upstream in
/// Raft commit, not the actor). Every field is a lock-free atomic updated on the
/// engine thread's hot path (one store per job) so sampling never contends it.
pub struct ActorStats {
    /// Global partition id this actor owns (the gauge label).
    pub partition: u64,
    /// Monotonic count of jobs the engine thread has fully executed. Flat under
    /// any freeze; the rate of change is the actor's true throughput.
    pub jobs: AtomicU64,
    /// Depth of the `High` (completion/read) queue.
    pub hi_depth: AtomicUsize,
    /// Depth of the `Low` (creation) queue (mirrors the create-admission gate's
    /// backlog signal).
    pub lo_depth: AtomicUsize,
    /// [`mono_ms`] at which the currently-running job started, or `0` when the
    /// thread is idle (parked in `pop`). A sampler computes the in-progress job's
    /// elapsed time as `mono_ms() - job_start_mono_ms`; an unbounded climb is the
    /// signature of a wedged single writer.
    pub job_start_mono_ms: AtomicU64,
    /// `true` while the engine thread is running; set `false` the instant its
    /// command loop exits for ANY reason (clean shutdown, or — the case we hunt —
    /// a panic inside a command that silently kills the single writer and hangs
    /// every subsequent `with().await` forever).
    pub alive: AtomicBool,
    /// Cumulative wall-clock nanoseconds this actor has spent *inside* a command
    /// (`job(journal)` — engine apply **plus** the journal write/fsync), summed
    /// over its whole life. The ~1 Hz monitor differences this against wall time
    /// across all a node's actors to derive **actor saturation ρ** = busy/elapsed:
    /// ρ→1 means the single writer is the wall (CPU-bound apply or disk-bound
    /// fsync — both are counted here because the fsync happens *inside* the timed
    /// region), ρ≪1 means throughput is limited upstream (a starved actor parks in
    /// `pop`, accruing no busy time). This is the cold-start-proof "are we the
    /// bottleneck" signal the admission compressor keys off — absolute, needs no
    /// learned baseline, and immune to external/worker strain by construction.
    pub busy_nanos: AtomicU64,
}

impl ActorStats {
    fn new(partition: u64) -> Arc<Self> {
        Arc::new(Self {
            partition,
            jobs: AtomicU64::new(0),
            hi_depth: AtomicUsize::new(0),
            lo_depth: AtomicUsize::new(0),
            job_start_mono_ms: AtomicU64::new(0),
            alive: AtomicBool::new(true),
            busy_nanos: AtomicU64::new(0),
        })
    }

    /// Elapsed milliseconds of the currently-running job, or `0` when the engine
    /// thread is idle (parked in `pop`). An unbounded climb is the signature of a
    /// wedged single writer.
    pub fn current_job_ms(&self) -> u64 {
        let start = self.job_start_mono_ms.load(Ordering::Relaxed);
        if start == 0 {
            0
        } else {
            mono_ms().saturating_sub(start)
        }
    }
}

/// Scheduling priority for a queued [`Job`].
///
/// The engine actor is a single writer shared by *creation* and *completion*:
/// `CreateInstance` and `CompleteJob`/`activateJobs`/etc all serialize on one
/// thread per partition. Under a flood of creates that share of the actor is
/// finite, so a high create rate can starve completion — the active-instance
/// backlog (created-but-not-completed) then runs away into congestion collapse.
/// This is acute under async durability, where the fsync latency that used to
/// implicitly rate-limit creation is gone.
///
/// Completion-priority breaks that: completion-side work (`High`) is always
/// dequeued ahead of creation (`Low`), so workers stay fed and the backlog drains
/// before any new instance is admitted. Creation thus self-throttles to the
/// spare actor capacity left after completion — exactly the admission control the
/// fsync brake used to provide, but without the throughput cost.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum Priority {
    /// Completion-side and all reads: activate/complete/fail/correlate/queries.
    High,
    /// Instance creation. Yields to any pending completion work.
    Low,
}

/// The engine actor's priority mailbox: a two-tier intake drained High-before-Low
/// by the single engine thread. The mutex is held only for the O(1) push/pop of a
/// boxed closure (never across the engine work itself), so — unlike the old
/// `RwLock<Journal>` — it never parks a tokio worker on a contended lock.
struct Mailbox {
    /// The engine's lifecycle lock (#1473) — see [`EngineLifecycle`].
    lifecycle: Arc<EngineLifecycle>,
    inner: Mutex<MailboxInner>,
    signal: Condvar,
    /// Heartbeat/liveness telemetry for this partition's engine thread. The
    /// mailbox owns it so both the producers (depth updates on push/pop) and the
    /// consumer loop (job count, in-progress-job stamp, alive flag) can reach it.
    stats: Arc<ActorStats>,
}

struct MailboxInner {
    hi: VecDeque<Job>,
    lo: VecDeque<Job>,
    /// Live [`DeepthiHandle`] count. When it hits zero with both queues drained,
    /// the consumer loop exits (mirrors an mpsc channel disconnect).
    producers: usize,
    /// Set (under this mutex) by [`Mailbox::fail_pending`] when the engine
    /// thread dies. Once closed, [`Mailbox::push`] rejects every submission by
    /// dropping the job — releasing its reply sender — so a request that passed
    /// an `is_alive` check just before the death can never land in an
    /// already-drained queue with no consumer (#1473 review).
    closed: bool,
}

impl Mailbox {
    fn new(partition: u64) -> Arc<Self> {
        Arc::new(Self {
            lifecycle: Arc::new(EngineLifecycle::default()),
            inner: Mutex::new(MailboxInner {
                hi: VecDeque::new(),
                lo: VecDeque::new(),
                producers: 1,
                closed: false,
            }),
            signal: Condvar::new(),
            stats: ActorStats::new(partition),
        })
    }

    /// Enqueues `job`, or — when the mailbox is closed (the engine thread is
    /// dead) — drops it, so its reply sender is released and the awaiting
    /// caller terminates instead of hanging. Closure and enqueue are decided
    /// under the same mutex as the drain, so no ordering can strand a job.
    fn push(&self, priority: Priority, job: Job) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        if g.closed {
            drop(g);
            drop(job);
            return;
        }
        match priority {
            Priority::High => {
                g.hi.push_back(job);
                self.stats.hi_depth.store(g.hi.len(), Ordering::Relaxed);
            }
            Priority::Low => {
                g.lo.push_back(job);
                self.stats.lo_depth.store(g.lo.len(), Ordering::Relaxed);
            }
        }
        drop(g);
        self.signal.notify_one();
    }

    /// Blocks until a job is available, returning High-priority work first.
    /// Returns the job together with the [`Priority`] queue it came from (so the
    /// latency instrumentation can attribute the sample to its command class —
    /// creates vs completions). Returns `None` once every producer handle is gone
    /// and both queues are empty (clean shutdown).
    fn pop(&self) -> Option<(Job, Priority)> {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        loop {
            if let Some(job) = g.hi.pop_front() {
                self.stats.hi_depth.store(g.hi.len(), Ordering::Relaxed);
                return Some((job, Priority::High));
            }
            if let Some(job) = g.lo.pop_front() {
                self.stats.lo_depth.store(g.lo.len(), Ordering::Relaxed);
                return Some((job, Priority::Low));
            }
            if g.producers == 0 {
                return None;
            }
            g = self.signal.wait(g).unwrap_or_else(|e| e.into_inner());
        }
    }

    /// Drops every queued job, releasing each one's resources — a `with` job's
    /// reply sender, so its awaiting caller terminates (its `reply_rx` resolves
    /// to `Err`) instead of hanging, and with it any engine-lifecycle guard that
    /// caller holds. Called when the engine thread dies: no consumer will ever
    /// drain these, so a queued apply / snapshot-capture would otherwise park
    /// its caller (and its shared lifecycle guard) forever, deadlocking a
    /// rebuild's `retire()` against the dead actor (#1473 review).
    fn fail_pending(&self) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.closed = true;
        // Move the queued jobs OUT under the lock, then drop them only after
        // releasing it: a queued closure may own a `DeepthiHandle` clone whose
        // `Drop` re-acquires this same (non-reentrant) mutex — dropping it while
        // held would self-deadlock the death handler (#1473 review).
        let hi = std::mem::take(&mut g.hi);
        let lo = std::mem::take(&mut g.lo);
        self.stats.hi_depth.store(0, Ordering::Relaxed);
        self.stats.lo_depth.store(0, Ordering::Relaxed);
        drop(g);
        drop(hi);
        drop(lo);
    }
}

/// The single per-engine LIFECYCLE LOCK (#1473).
///
/// An engine actor outlives the Raft member that drives it: a partition rebuild
/// retires the predecessor member and hands the same engine (or a journal
/// swapped into it from durable state) to the successor. Every operation that
/// must never interleave with that hand-over takes this one lock, instead of
/// each operation carrying its own bespoke fence/counter/gate:
///
/// - **Shared** ([`DeepthiHandle::lifecycle_enter`] for a Raft member bound to
///   an epoch, [`DeepthiHandle::lifecycle_shared`] for epoch-agnostic work):
///   Raft `apply`, snapshot capture / build+publish / install, and deployment
///   fan-out into the engine. A member-bound entry is refused (`None`) once the
///   epoch it was constructed in has been retired, so a late predecessor
///   operation can never touch the engine or the snapshot dir again.
/// - **Exclusive** ([`DeepthiHandle::lifecycle_exclusive`]): retiring the
///   member (fence + drain) and the rebuild's engine resolution — the health
///   probe, the durable reconstruction, and the journal swap. Acquiring it
///   retires the current epoch (every member bound to it is fenced) and waits
///   until no shared holder is in flight; tokio's `RwLock` is write-preferring,
///   so new shared entries queue behind it and then observe the new epoch.
///
/// **Lock order** (deadlock freedom): engine lifecycle lock → the journal's
/// segmented checkpoint permit. A snapshot build takes the build semaphore
/// BEFORE the lifecycle lock and never waits on the semaphore while holding it.
/// No holder of the lifecycle lock ever waits on another engine's lifecycle
/// lock, and the exclusive holder never waits on a Raft
/// apply (the member's core is shut down first). Callers bound the exclusive
/// acquisition with a timeout so a wedged shared holder fails the rebuild
/// closed (the supervisor retries) rather than hanging it.
#[derive(Default)]
pub struct EngineLifecycle {
    lock: Arc<tokio::sync::RwLock<()>>,
    epoch: AtomicU64,
    /// Serializes snapshot PUBLICATION with snapshot reaping across the local
    /// build path and the follower install path (#1488 review): both paths
    /// publish (the durable `current-snapshot.json` pointer + the
    /// current-snapshot cache) and then reap every other `snap-*` artifact,
    /// and each path's in-flight guard drops when it returns — so without
    /// this lock a paused publisher's sweep could delete the file a
    /// just-finished publisher made the durable recovery point, leaving
    /// recovery without that snapshot after the covered log is purged.
    /// Acquired AFTER the shared lifecycle read guard (lock order:
    /// lifecycle → snapshot publish).
    snapshot_publish: Arc<tokio::sync::Mutex<()>>,
}

/// A shared hold on an engine's lifecycle lock ([`EngineLifecycle`]).
pub type LifecycleShared = tokio::sync::OwnedRwLockReadGuard<()>;
/// An exclusive hold on an engine's lifecycle lock ([`EngineLifecycle`]).
pub type LifecycleExclusive = tokio::sync::OwnedRwLockWriteGuard<()>;
/// A hold on an engine's snapshot publication lock ([`EngineLifecycle`]).
pub type SnapshotPublishGuard = tokio::sync::OwnedMutexGuard<()>;

/// A cloneable handle to the engine actor. All engine mutation and engine-state
/// reads go through here; the [`Journal`] lives on a single dedicated thread, so
/// no async task ever blocks a tokio worker on a contended lock.
pub struct DeepthiHandle {
    mb: Arc<Mailbox>,
}

impl Clone for DeepthiHandle {
    fn clone(&self) -> Self {
        self.mb
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .producers += 1;
        Self {
            mb: Arc::clone(&self.mb),
        }
    }
}

impl Drop for DeepthiHandle {
    fn drop(&mut self) {
        let mut g = self.mb.inner.lock().unwrap_or_else(|e| e.into_inner());
        g.producers -= 1;
        if g.producers == 0 {
            drop(g);
            // Wake the consumer so it observes shutdown and drops the Journal
            // (flushing its writer).
            self.mb.signal.notify_all();
        }
    }
}

/// Fires when the engine thread's command loop unwinds — whether by clean
/// shutdown or (the case we hunt) a **panic inside a command**. Closes and
/// drains the mailbox, then clears the [`ActorStats::alive`] flag and logs
/// loudly so a silently-dead single writer can never again masquerade as a
/// mysterious completion-freeze. The panic's payload/location is captured
/// separately by the process-wide panic hook installed in `main`.
///
/// It also DRAINS every still-queued request ([`Mailbox::fail_pending`]): a
/// `with` job queued behind the dead consumer would otherwise never run, so its
/// caller's `reply_rx` would never resolve — parking the caller (and any
/// engine-lifecycle guard it holds) forever and deadlocking a rebuild's
/// `retire()` against the dead actor (#1473 review). Dropping the queued jobs
/// releases their reply senders, so each pending caller terminates (its
/// `reply_rx` resolves to `Err`) and releases its guard.
///
/// ORDER MATTERS: the mailbox is closed and drained BEFORE `alive` flips to
/// `false`, so a caller that observes `is_alive() == false` is guaranteed the
/// mailbox already rejects new submissions — no window in which a request
/// lands in a not-yet-closed queue whose drain already ran (#1473 review).
struct AliveGuard {
    stats: Arc<ActorStats>,
    mb: Arc<Mailbox>,
}

impl Drop for AliveGuard {
    fn drop(&mut self) {
        // Terminate every pending request BEFORE publishing death or logging,
        // so a waiting caller's guard is released even if the log sink blocks,
        // and so `is_alive() == false` implies the mailbox is already closed.
        self.mb.fail_pending();
        self.stats.alive.store(false, Ordering::Relaxed);
        let panicking = thread::panicking();
        tracing::error!(
            partition = self.stats.partition,
            jobs = self.stats.jobs.load(Ordering::Relaxed),
            panicking,
            "engine actor thread exited: the single writer for this partition is \
             DOWN — every subsequent create/complete on it will hang forever \
             (completion-freeze). See the panic hook for the cause if panicking=true."
        );
    }
}

impl DeepthiHandle {
    /// Spawns the engine thread that owns `journal` (owner of `partition`) and
    /// returns a handle to it. The thread runs until every [`DeepthiHandle`] clone
    /// is dropped (the channel closes), then drops the [`Journal`] — flushing its
    /// writer thread. The thread is detached: durability never depends on a clean
    /// shutdown because callers `await` each command's
    /// [`Commit`](crate::journal::Commit) before acknowledging, so an acknowledged
    /// write is already fsynced even on a hard kill.
    ///
    /// When `controller` is `Some`, the loop times every command and feeds the
    /// latency to the adaptive backpressure limiter (which sizes the in-flight
    /// watermark from observed per-command latency).
    pub fn spawn(
        mut journal: Journal,
        partition: u64,
        controller: Option<AdaptiveController>,
    ) -> Self {
        let mb = Mailbox::new(partition);
        let consumer = Arc::clone(&mb);
        let profile = std::env::var_os("NANOBPM_ACTOR_PROFILE").is_some();
        // Startup-only (partition boot / lazy replica engine): a thread-spawn
        // failure is OS resource exhaustion at boot, which is unrecoverable —
        // the partition can never serve writes — so we fail fast rather than
        // surface a half-initialized node.
        #[allow(
            clippy::expect_used,
            reason = "startup-only: OS thread-spawn failure at partition boot is unrecoverable resource exhaustion"
        )]
        thread::Builder::new()
            .name("nanobpmn-engine".into())
            .spawn(move || {
                // Closes + drains pending requests, then clears `alive` and
                // logs on exit/panic-unwind (drop-guard, so it fires even when
                // a command panics through the loop).
                let _alive = AliveGuard {
                    stats: Arc::clone(&consumer.stats),
                    mb: Arc::clone(&consumer),
                };
                if profile || controller.is_some() {
                    Self::run_instrumented(&consumer, &mut journal, profile, controller);
                } else {
                    let stats = &consumer.stats;
                    while let Some((job, _)) = consumer.pop() {
                        stats.job_start_mono_ms.store(mono_ms(), Ordering::Relaxed);
                        let before_job = Instant::now();
                        job(&mut journal);
                        stats
                            .busy_nanos
                            .fetch_add(before_job.elapsed().as_nanos() as u64, Ordering::Relaxed);
                        stats.job_start_mono_ms.store(0, Ordering::Relaxed);
                        stats.jobs.fetch_add(1, Ordering::Relaxed);
                    }
                }
            })
            .expect("spawn engine thread");
        Self { mb }
    }

    /// Command loop with per-command timing. Drives two optional consumers of the
    /// latency signal: the adaptive backpressure `controller` (per command) and,
    /// when `profile` is set, utilization logging (per 5 s window). Timing each
    /// `recv` (idle, queue empty = actor starved) and each job (busy) lets us tell
    /// whether the single writer is the bottleneck (busy≈100%) or whether
    /// throughput is limited upstream of it (busy≪100% = latency/client bound).
    fn run_instrumented(
        mb: &Mailbox,
        journal: &mut Journal,
        profile: bool,
        mut controller: Option<AdaptiveController>,
    ) {
        use std::time::Duration;
        const WINDOW: Duration = Duration::from_secs(5);

        let mut busy = Duration::ZERO;
        let mut idle = Duration::ZERO;
        let mut jobs: u64 = 0;
        let mut window_start = Instant::now();

        loop {
            let before_recv = Instant::now();
            let Some((job, prio)) = mb.pop() else {
                break;
            };
            idle += before_recv.elapsed();

            mb.stats
                .job_start_mono_ms
                .store(mono_ms(), Ordering::Relaxed);
            let before_job = Instant::now();
            job(journal);
            let job_time = before_job.elapsed();
            mb.stats.job_start_mono_ms.store(0, Ordering::Relaxed);
            mb.stats.jobs.fetch_add(1, Ordering::Relaxed);
            // Publish busy time for node-level actor-saturation ρ (always — this is
            // the admission compressor's control signal, not profiling output).
            mb.stats
                .busy_nanos
                .fetch_add(job_time.as_nanos() as u64, Ordering::Relaxed);

            // Feed the adaptive limiter every command, tagged with its class.
            // Both classes report pure apply time (`job_time`): creates (`Low`)
            // drive the create-processing concurrency limiter, completion-side
            // (`High`) drives the worker governor. The admission-backlog cap is
            // driven separately by the monitor-stepped backlog governor off e2e
            // job sojourn, not from here.
            if let Some(c) = controller.as_mut() {
                let is_create = prio == Priority::Low;
                c.record(job_time, is_create);
            }

            if !profile {
                continue;
            }
            busy += job_time;
            jobs += 1;

            let elapsed = window_start.elapsed();
            if elapsed >= WINDOW {
                let total = busy + idle;
                let busy_pct = if total.is_zero() {
                    0.0
                } else {
                    busy.as_secs_f64() / total.as_secs_f64() * 100.0
                };
                let jps = jobs as f64 / elapsed.as_secs_f64();
                let avg_us = if jobs > 0 {
                    busy.as_micros() as f64 / jobs as f64
                } else {
                    0.0
                };
                tracing::info!(
                    "actor: {jps:.0} jobs/s, busy {busy_pct:.1}%, avg {avg_us:.1}us/job ({jobs} jobs/window)"
                );
                busy = Duration::ZERO;
                idle = Duration::ZERO;
                jobs = 0;
                window_start = Instant::now();
            }
        }
    }

    /// Runs `f` on the engine thread at [`Priority::High`] and awaits its result.
    /// This is the default for every completion-side and read command; only
    /// instance creation uses [`with_low`](DeepthiHandle::with_low). The closure
    /// receives exclusive `&mut Journal`. Keep `f` cheap: do serde-heavy request
    /// decode and response encode on the caller side (before/after the call) so
    /// they run in parallel off the single engine thread.
    pub async fn with<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Journal) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.with_priority(Priority::High, f).await
    }

    /// Fallible [`with`](DeepthiHandle::with): returns `None` instead of
    /// panicking when the engine actor is dead and can never answer — either
    /// already dead when the job is queued, or unwound while the job sat in the
    /// mailbox (its [`AliveGuard`] drains the queue, dropping the job's reply
    /// sender, so the receiver resolves to `Err`).
    ///
    /// Use this on every path that must SURVIVE the actor's death — the raft
    /// rebuild supervisor's health probe, recovery-metadata read, and journal
    /// swap (#1473): a `with` there would panic on the dead-actor `expect`,
    /// unwinding the supervisor task and halting recovery for EVERY partition,
    /// where `None` just fails this partition's pass (the supervisor retries).
    pub async fn try_with<R, F>(&self, f: F) -> Option<R>
    where
        F: FnOnce(&mut Journal) -> R + Send + 'static,
        R: Send + 'static,
    {
        if !self.is_alive() {
            return None;
        }
        let (reply_tx, reply_rx) = oneshot::channel();
        let job: Job = Box::new(move |journal| {
            // A dropped receiver (caller cancelled) is fine; the command still ran.
            let _ = reply_tx.send(f(journal));
        });
        self.mb.push(Priority::High, job);
        // `Err` means the sender was dropped without answering: the actor is
        // dead and its `AliveGuard` drained the queue (the panic hook already
        // captured the cause).
        reply_rx.await.ok()
    }

    /// Like [`with`](DeepthiHandle::with) but at [`Priority::Low`] — the engine
    /// services it only when no completion-side work is queued. Used by the
    /// instance-create path so a flood of creates cannot starve completion and
    /// run the active-instance backlog away into congestion collapse.
    pub async fn with_low<R, F>(&self, f: F) -> R
    where
        F: FnOnce(&mut Journal) -> R + Send + 'static,
        R: Send + 'static,
    {
        self.with_priority(Priority::Low, f).await
    }

    /// Runs `f` on the engine thread at the given [`Priority`] and awaits its
    /// result.
    pub async fn with_priority<R, F>(&self, priority: Priority, f: F) -> R
    where
        F: FnOnce(&mut Journal) -> R + Send + 'static,
        R: Send + 'static,
    {
        let (reply_tx, reply_rx) = oneshot::channel();
        let job: Job = Box::new(move |journal| {
            // A dropped receiver (caller cancelled) is fine; the command still ran.
            let _ = reply_tx.send(f(journal));
        });
        self.mb.push(priority, job);
        // A `None` here means the engine thread unwound mid-command (its
        // `AliveGuard` already cleared `ActorStats::alive` and logged the panic,
        // and the process-wide panic hook captured the cause). The single writer
        // is dead, so no reply can ever arrive — there is no typed error to
        // propagate through the `R`-returning API, and failing fast keeps a
        // wedged partition from hanging every caller forever.
        #[allow(
            clippy::expect_used,
            reason = "a dead engine thread can never answer; AliveGuard + the panic hook already surfaced the fatal unwind"
        )]
        reply_rx.await.expect("engine thread answered")
    }

    /// Enqueues `f` to run on the engine thread (at [`Priority::High`]) without
    /// awaiting a result. Used by background maintenance (e.g. the read-model
    /// exporter's hot-state eviction) that has no reply to deliver and must not
    /// block; eviction drains the backlog, so it belongs on the completion tier.
    pub fn spawn_job<F>(&self, f: F)
    where
        F: FnOnce(&mut Journal) + Send + 'static,
    {
        self.mb.push(Priority::High, Box::new(f));
    }

    /// Current depth of this partition's `Low` (creation) queue: creates that have
    /// been submitted but not yet applied because the engine is busy with
    /// completion-side work. The create-admission gate reads this to bound how
    /// long a create can wait (hence create-side latency) under overload. A relaxed
    /// load — an approximate bound is sufficient.
    pub fn pending_low(&self) -> usize {
        self.mb.stats.lo_depth.load(Ordering::Relaxed)
    }

    /// This partition's engine-actor heartbeat/liveness telemetry. Sampled ~1 Hz
    /// by the metrics monitor to publish the `nanobpm_actor_*` gauges that
    /// distinguish a dead / wedged / idle single writer under sustained load.
    pub fn stats(&self) -> &Arc<ActorStats> {
        &self.mb.stats
    }

    /// Whether this handle's engine actor thread is still running (#1473).
    /// `false` the instant the actor's command loop exits for ANY reason (clean
    /// shutdown or a panic inside a command — the `AliveGuard` clears it, and
    /// only AFTER it closed and drained the mailbox, so observing `false`
    /// guarantees a new submission is rejected rather than stranded). A caller
    /// about to reuse a handle for a long-lived rebuild MUST check this first:
    /// [`with`](Self::with) on a dead actor is rejected by the closed mailbox
    /// (its reply sender drops, so the await panics on the dead-actor
    /// `expect`) — use [`try_with`](Self::try_with) on any path that must
    /// survive the actor's death.
    pub fn is_alive(&self) -> bool {
        self.mb.stats.alive.load(Ordering::Relaxed)
    }

    /// The engine's current lifecycle epoch ([`EngineLifecycle`]). A Raft
    /// member records it at construction and passes it to
    /// [`lifecycle_enter`](Self::lifecycle_enter) for every operation.
    pub fn lifecycle_epoch(&self) -> u64 {
        self.mb.lifecycle.epoch.load(Ordering::SeqCst)
    }

    /// Enters the lifecycle lock SHARED on behalf of a member bound to `epoch`.
    /// `None` when that epoch has been retired — the caller must not touch the
    /// engine or its snapshot dir. The epoch only changes under the exclusive
    /// lock, so it cannot change while the returned guard is held.
    pub async fn lifecycle_enter(&self, epoch: u64) -> Option<LifecycleShared> {
        let guard = Arc::clone(&self.mb.lifecycle.lock).read_owned().await;
        (self.lifecycle_epoch() == epoch).then_some(guard)
    }

    /// Enters the lifecycle lock SHARED for epoch-agnostic engine work
    /// (deployment fan-out): excluded only from a concurrent rebuild's engine
    /// resolution, so the work lands either before the durable read or in the
    /// swapped-in engine — never in between.
    pub async fn lifecycle_shared(&self) -> LifecycleShared {
        Arc::clone(&self.mb.lifecycle.lock).read_owned().await
    }

    /// Enters the lifecycle lock EXCLUSIVE and retires the current epoch. Wrap
    /// in a timeout: it waits for every shared holder to leave.
    pub async fn lifecycle_exclusive(&self) -> LifecycleExclusive {
        let guard = Arc::clone(&self.mb.lifecycle.lock).write_owned().await;
        self.mb.lifecycle.epoch.fetch_add(1, Ordering::SeqCst);
        guard
    }

    /// Acquires the engine's snapshot publication lock (#1488 review). A
    /// snapshot build/install holds it across its publish (durable pointer +
    /// current-snapshot cache) AND its superseded-artifact reap, so a reap
    /// can never delete the file a concurrent publish just made the durable
    /// recovery point. Lock order: lifecycle (shared) → snapshot publish.
    pub async fn snapshot_publish_lock(&self) -> SnapshotPublishGuard {
        Arc::clone(&self.mb.lifecycle.snapshot_publish)
            .lock_owned()
            .await
    }

    /// Whether `self` and `other` drive the same engine lifecycle.
    pub fn same_lifecycle(&self, other: &DeepthiHandle) -> bool {
        Arc::ptr_eq(&self.mb.lifecycle, &other.mb.lifecycle)
    }
}

#[cfg(test)]
mod tests {
    use std::time::Duration;

    use super::*;
    use crate::journal::Journal;

    fn wait_until(mut cond: impl FnMut() -> bool) {
        for _ in 0..300 {
            if cond() {
                return;
            }
            thread::sleep(Duration::from_millis(10));
        }
        panic!("condition not met within 3s");
    }

    #[test]
    fn heartbeat_counts_jobs_and_reports_alive_and_partition() {
        let h = DeepthiHandle::spawn(Journal::in_memory_partition(3), 3, None);
        let stats = Arc::clone(h.stats());
        assert_eq!(stats.partition, 3);
        assert!(stats.alive.load(Ordering::Relaxed));

        for _ in 0..5 {
            h.spawn_job(|_journal| {});
        }
        wait_until(|| stats.jobs.load(Ordering::Relaxed) >= 5);
        // An idle (parked) actor reports no in-progress job.
        assert_eq!(stats.current_job_ms(), 0);
        assert!(stats.alive.load(Ordering::Relaxed));
    }

    #[test]
    fn current_job_ms_climbs_while_a_job_runs() {
        let h = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let stats = Arc::clone(h.stats());
        // A job that sleeps mimics a wedged single writer.
        h.spawn_job(|_journal| thread::sleep(Duration::from_millis(150)));
        wait_until(|| stats.current_job_ms() >= 50);
        assert!(
            stats.current_job_ms() >= 50,
            "in-progress job time is visible"
        );
        // Once it finishes the actor is idle again.
        wait_until(|| stats.jobs.load(Ordering::Relaxed) >= 1);
        wait_until(|| stats.current_job_ms() == 0);
    }

    #[test]
    fn panicking_job_flips_alive_without_poisoning_the_mailbox() {
        let h = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let stats = Arc::clone(h.stats());
        // A command that panics kills the single writer thread.
        h.spawn_job(|_journal| panic!("boom"));
        wait_until(|| !stats.alive.load(Ordering::Relaxed));
        assert!(!stats.alive.load(Ordering::Relaxed));
        // The panic happened with the mailbox lock released, so producers can
        // still submit (mutex is NOT poisoned) — submission must not panic even
        // though the consumer is gone. The closed mailbox rejects the job rather
        // than stranding it in a queue nobody drains (#1473 review).
        h.spawn_job(|_journal| {});
        assert_eq!(h.stats().hi_depth.load(Ordering::Relaxed), 0);
    }

    /// A poisoned mailbox mutex (a panic while the guard is held) must not
    /// wedge the actor: push/pop recover the guard and keep the queue
    /// serviceable.
    #[test]
    fn mailbox_survives_poisoned_lock() {
        let mb = Mailbox::new(0);
        {
            let inner: &Mutex<MailboxInner> = &mb.inner;
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                let _g = inner.lock();
                panic!("poison mailbox");
            }));
        }
        // Producers and the consumer both recover the poisoned guard.
        mb.push(Priority::High, Box::new(|_journal| {}));
        let (job, prio) = mb.pop().expect("job enqueued before poison is served");
        assert_eq!(prio, Priority::High);
        drop(job);
    }

    /// #1473 (review): when the engine actor dies with a `with` request still
    /// QUEUED, the pending request is terminated — its reply sender is dropped,
    /// so the awaiting caller's `reply_rx` resolves (to `Err`, surfacing as a
    /// panic at the `expect`) instead of hanging forever, and any
    /// engine-lifecycle guard the caller holds is released with it. This is
    /// what lets a rebuild's `retire()` drain past a dead actor instead of
    /// timing out on a guard that can never leave.
    #[test]
    fn actor_death_terminates_a_queued_request_and_releases_its_guard() {
        let h = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let stats = Arc::clone(h.stats());

        // A job that parks the actor until released, then PANICS — dying while
        // the `with` below is still queued behind it. The park guarantees the
        // `with` is enqueued (not yet served) at the moment of the unwind.
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let (block_tx, block_rx) = std::sync::mpsc::channel::<()>();
        h.spawn_job(move |_journal| {
            let _ = started_tx.send(()); // the actor is now INSIDE this job
            let _ = block_rx.recv(); // park until the test releases us
            panic!("boom"); // die with the `with` still queued behind us
        });
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the blocking job is running");

        // The queued request: its caller holds a lifecycle guard across the
        // await — the exact shape the rebuild's retire() must be able to drain.
        let guard_holder = h.clone();
        let caller = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("rt");
            // The `with` await panics once the actor is dead (its pending job's
            // reply sender is dropped, closing the oneshot). Catch that panic so
            // the thread returns normally; the caller's guard drops with the
            // frame either way — it is NOT held forever.
            let _ = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                rt.block_on(async move {
                    let _guard = guard_holder.lifecycle_shared().await;
                    // Queues behind the parked job; terminated by the actor's
                    // death rather than hanging on a reply that can never arrive.
                    guard_holder.with(|_j| ()).await;
                });
            }));
        });
        // Wait until the `with` request is actually QUEUED behind the parked job
        // (the parked job was already popped, so depth 1 == the `with`).
        wait_until(|| h.stats().hi_depth.load(Ordering::Relaxed) >= 1);

        // Release the parked job: it panics, unwinding the actor with the
        // caller's `with` still queued behind it.
        let _ = block_tx.send(());
        wait_until(|| !stats.alive.load(Ordering::Relaxed));
        assert!(!stats.alive.load(Ordering::Relaxed));

        // The queued request's caller terminated (its guard released) rather
        // than hanging on a reply that can never arrive.
        caller
            .join()
            .expect("a queued request is terminated when the actor dies");
    }

    /// #1473 (review): `try_with` is the FALLIBLE request the rebuild
    /// supervisor's probe / metadata read / swap use. It must return `None` —
    /// never panic, never hang — both when the actor is already dead and when
    /// it dies with the request still queued, so the supervisor task survives
    /// and keeps recovering the other partitions.
    #[test]
    fn try_with_returns_none_on_a_dead_actor_without_panicking() {
        let h = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let stats = Arc::clone(h.stats());

        // Already-dead actor: `None` immediately, nothing queued.
        h.spawn_job(|_journal| panic!("boom"));
        wait_until(|| !stats.alive.load(Ordering::Relaxed));
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("rt");
        let dead = h.clone();
        let answered = rt.block_on(async move { dead.try_with(|_j| 1u32).await });
        assert_eq!(answered, None, "a dead actor never answers");
        assert_eq!(
            h.stats().hi_depth.load(Ordering::Relaxed),
            0,
            "an already-dead actor's request is never queued"
        );

        // Live actor that dies with the `try_with` still queued behind a parked
        // job: the drain drops the reply sender, so the await resolves to None.
        let h2 = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let stats2 = Arc::clone(h2.stats());
        let (started_tx, started_rx) = std::sync::mpsc::channel::<()>();
        let (block_tx, block_rx) = std::sync::mpsc::channel::<()>();
        h2.spawn_job(move |_journal| {
            let _ = started_tx.send(());
            let _ = block_rx.recv();
            panic!("boom");
        });
        started_rx
            .recv_timeout(Duration::from_secs(5))
            .expect("the blocking job is running");
        let caller_handle = h2.clone();
        let caller = thread::spawn(move || {
            let rt = tokio::runtime::Builder::new_current_thread()
                .enable_all()
                .build()
                .expect("rt");
            rt.block_on(async move { caller_handle.try_with(|_j| 2u32).await })
        });
        wait_until(|| h2.stats().hi_depth.load(Ordering::Relaxed) >= 1);
        let _ = block_tx.send(());
        wait_until(|| !stats2.alive.load(Ordering::Relaxed));
        let answered = caller
            .join()
            .expect("try_with never panics on a dead actor");
        assert_eq!(answered, None, "a queued try_with resolves to None");
    }

    /// #1473 (review): the check-before-death / enqueue-after-drain ordering.
    /// A request that passed `is_alive` just before the actor died is pushed
    /// AFTER `fail_pending` drained the queue. The closed mailbox must reject
    /// it (dropping its reply sender) rather than strand it with no consumer.
    #[test]
    fn push_after_the_drain_is_rejected_not_stranded() {
        let h = DeepthiHandle::spawn(Journal::in_memory_partition(0), 0, None);
        let stats = Arc::clone(h.stats());
        h.spawn_job(|_journal| panic!("boom"));
        wait_until(|| !stats.alive.load(Ordering::Relaxed));

        // Simulate the late enqueue directly (bypassing the is_alive fast path).
        let (reply_tx, mut reply_rx) = oneshot::channel::<u32>();
        h.mb.push(
            Priority::High,
            Box::new(move |_journal| {
                let _ = reply_tx.send(3);
            }),
        );
        assert_eq!(
            h.stats().hi_depth.load(Ordering::Relaxed),
            0,
            "a closed mailbox never queues"
        );
        assert!(
            matches!(
                reply_rx.try_recv(),
                Err(oneshot::error::TryRecvError::Closed)
            ),
            "the rejected job's reply sender is released, so the caller terminates"
        );
    }
}
