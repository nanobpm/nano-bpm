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
}

impl Mailbox {
    fn new(partition: u64) -> Arc<Self> {
        Arc::new(Self {
            inner: Mutex::new(MailboxInner {
                hi: VecDeque::new(),
                lo: VecDeque::new(),
                producers: 1,
            }),
            signal: Condvar::new(),
            stats: ActorStats::new(partition),
        })
    }

    fn push(&self, priority: Priority, job: Job) {
        let mut g = self.inner.lock().unwrap_or_else(|e| e.into_inner());
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
}

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
/// shutdown or (the case we hunt) a **panic inside a command**. Clears the
/// [`ActorStats::alive`] flag and logs loudly so a silently-dead single writer
/// can never again masquerade as a mysterious completion-freeze. The panic's
/// payload/location is captured separately by the process-wide panic hook
/// installed in `main`.
struct AliveGuard(Arc<ActorStats>);

impl Drop for AliveGuard {
    fn drop(&mut self) {
        self.0.alive.store(false, Ordering::Relaxed);
        let panicking = thread::panicking();
        tracing::error!(
            partition = self.0.partition,
            jobs = self.0.jobs.load(Ordering::Relaxed),
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
                // Clears `alive` + logs on exit/panic-unwind (drop-guard, so it
                // fires even when a command panics through the loop).
                let _alive = AliveGuard(Arc::clone(&consumer.stats));
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
        // still enqueue (mutex is NOT poisoned) — enqueue must not panic even
        // though the consumer is gone.
        h.spawn_job(|_journal| {});
        assert!(h.stats().hi_depth.load(Ordering::Relaxed) >= 1);
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
}
