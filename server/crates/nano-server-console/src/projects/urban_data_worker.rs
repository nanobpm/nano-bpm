//! One long-lived `urban data --serve` worker per Urban project (#1836).
//!
//! A single-shot gateway spawns a fresh Node process (pair) for
//! every Data-panel op; #1329 bounded how many run at once, but each op still
//! paid a cold Node start. `@nanobpm/urban` >= 0.100.0 ships a serve mode
//! (nanobpm/nano-ide#627): one JSON request per stdin line carrying an `id`,
//! one JSON reply per stdout line echoing it, errors as replies, a clean exit
//! on stdin EOF. This module keeps at most one such worker alive per project
//! directory and pipes every op through it, so once the worker is warm no data
//! request starts a new process.
//!
//! Lifecycle:
//! - **Spawned lazily** on the first op for a project.
//! - **Bounded**: the registry lives on the [`DataGatewayLimiter`], and every
//!   admitted op (serve *or* the embedded piped gateway) evicts least-recently-used idle
//!   workers so `idle resident workers + running ops <= concurrency` — #1329's
//!   console-wide live-process bound holds. Workers idle past [`IDLE_TTL`] are
//!   retired on the next sweep too.
//! - **Restarted on crash**: a worker that has exited (or whose pipe broke /
//!   reached EOF mid-op) is reaped and a fresh one is spawned for the next op.
//! - **Restarted on toolkit/manifest change**: each op recomputes
//!   [`toolkit_fingerprint`] ([`crate::urban::toolkit_identity`] — the binary,
//!   its target, and its enclosing package's `package.json` + `dist/cli.js` —
//!   plus the `nano.app.json` bytes and the directory's identity); a mismatch
//!   retires the old worker first.
//! - **Cancellation-safe**: an op cancelled mid-exchange kills its worker's
//!   process group at once (drop guard) and reaps it in the background, so a
//!   hung or abandoned write never keeps running or holding locks.
//! - **One deadline per op**: waiting for the project's worker, for gateway
//!   admission and for the reply all share the limiter's single timeout; the
//!   per-project wait queue is bounded (Busy back-pressure) and holds no
//!   execution permit.
//! - **Retired with its project**: deleting or renaming a project calls
//!   [`retire_project_worker_blocking`], which tombstones the path, kills the
//!   idle *and* any in-flight worker's process group, and waits for every op on
//!   the slot to leave before the directory is touched. Ops on a tombstoned path
//!   fail fast and never spawn; a worker of a retired slot is never parked.
//!   A retired slot stays discoverable until every op and teardown on it has
//!   gone, so a retry after a timed-out retirement fails closed too.
//! - **No replacement beside a survivor**: a worker that outlives its kill
//!   grace keeps a hold on its slot; the next spawn for that project waits
//!   (within the op deadline) until it has really exited.
//! - **Bounded protocol reads**: a stdout reply line longer than
//!   [`MAX_REPLY_LINE_BYTES`] is a protocol error that kills the worker; stderr
//!   is drained in bounded chunks.
//! - **One request in flight per worker** (the project turn). That is what makes
//!   accepting the worker's `{id:null}` parse-error reply as ours safe.
//! - **Orphan-free**: the console holds the worker's stdin, so if the console
//!   dies the worker sees EOF and exits; timeouts kill its whole process group.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use super::{DataError, DataGatewayLimiter};

/// How much of the worker's most recent stderr we keep to explain a crash.
const STDERR_TAIL_BYTES: usize = 2048;
/// Grace for a retired worker to exit on stdin EOF before it is killed.
const RETIRE_GRACE: Duration = Duration::from_secs(2);
/// An idle worker unused for this long is retired on the next sweep.
const IDLE_TTL: Duration = Duration::from_secs(300);
/// Longest stdout reply line accepted from a worker; longer is a protocol
/// error (the worker is killed) rather than unbounded console memory.
const MAX_REPLY_LINE_BYTES: usize = 16 * 1024 * 1024;
/// How long project retirement waits for in-flight ops (already killed) to
/// leave the slot before failing the delete/rename closed.
const RETIRE_DRAIN: Duration = Duration::from_secs(15);

struct Worker {
    child: Child,
    /// The launcher's pid, recorded at spawn — on Unix also its process-group
    /// id. Teardown kills by this, never by `child.id()`, which is `None` once
    /// the leader has been reaped while forked descendants may live on.
    pid: Option<u32>,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    fingerprint: u64,
    next_id: u64,
    /// Set while a request is written but its reply not yet read; a worker
    /// found in this state is retired, never reused.
    in_flight: bool,
    last_used: Instant,
    stderr_tail: Arc<Mutex<String>>,
}

#[derive(Default)]
struct SlotInner {
    /// Ops waiting for (or holding) this project's turn, plus teardowns
    /// still owning one of the slot's processes.
    waiters: AtomicUsize,
    /// Teardowns of this slot's workers that have not yet seen the process
    /// exit. While non-zero no replacement worker is spawned (#1836).
    unreaped: AtomicUsize,
    /// Serialises ops for one project (one request in flight per worker).
    /// Holds no process: waiting on it pins nothing.
    turn: tokio::sync::Mutex<()>,
    /// The project's worker while it is **idle** (between ops). An op takes
    /// it out once admitted (it is then counted by the op's permit) and puts
    /// it back when done. Eviction reads this directly, so an idle worker is
    /// always evictable — even while an op holds the turn and waits for a
    /// permit (#1836: a locked-but-unadmitted worker must still count).
    idle: Mutex<Option<Worker>>,
    /// The in-use worker's pid and the slot's retired flag. An op records its
    /// worker here (and parks it) only while `retired` is false, under this
    /// lock; retirement sets `retired` under it, so it sees — and kills —
    /// every worker the slot will ever run.
    active: Mutex<Active>,
}

#[derive(Default)]
struct Active {
    pid: Option<u32>,
    retired: bool,
}

impl SlotInner {
    fn active(&self) -> std::sync::MutexGuard<'_, Active> {
        self.active.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn idle(&self) -> std::sync::MutexGuard<'_, Option<Worker>> {
        self.idle.lock().unwrap_or_else(|e| e.into_inner())
    }
}

type Slot = Arc<SlotInner>;

/// The per-project worker slots, keyed on the canonical project directory.
/// Owned by a [`DataGatewayLimiter`], so its budget bounds them.
#[derive(Default)]
pub(crate) struct WorkerRegistry {
    slots: Mutex<HashMap<PathBuf, Slot>>,
    /// Paths whose directory is being deleted/renamed (count of concurrent
    /// retirements). Always locked after `slots`.
    tombstones: Mutex<HashMap<PathBuf, usize>>,
    /// Slots removed by a retirement whose ops/teardowns may still be alive.
    /// While any of them holds a waiter its path stays unavailable (no new
    /// slot, no new retirement), even after a timed-out retirement lifted its
    /// tombstone (#1836). Always locked after `tombstones`.
    retiring: Mutex<HashMap<PathBuf, Vec<Slot>>>,
}

impl WorkerRegistry {
    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<PathBuf, Slot>> {
        self.slots.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn tombstones(&self) -> std::sync::MutexGuard<'_, HashMap<PathBuf, usize>> {
        self.tombstones.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn retiring(&self) -> std::sync::MutexGuard<'_, HashMap<PathBuf, Vec<Slot>>> {
        self.retiring.lock().unwrap_or_else(|e| e.into_inner())
    }

    /// Whether a retired slot for `dir` still has an op or teardown alive;
    /// prunes the drained ones.
    fn retiring_busy(&self, dir: &Path) -> bool {
        let mut r = self.retiring();
        let Some(v) = r.get_mut(dir) else {
            return false;
        };
        v.retain(|s| s.waiters.load(Ordering::Acquire) > 0);
        if v.is_empty() {
            r.remove(dir);
            return false;
        }
        true
    }

    #[cfg(all(test, unix))]
    fn slot_for(&self, dir: &Path) -> Slot {
        self.map().entry(dir.to_path_buf()).or_default().clone()
    }

    /// The slot for `dir`, unless its directory is being deleted/renamed.
    fn live_slot_for(&self, dir: &Path) -> Option<Slot> {
        let mut map = self.map();
        if self.tombstones().contains_key(dir) || self.retiring_busy(dir) {
            return None;
        }
        Some(map.entry(dir.to_path_buf()).or_default().clone())
    }

    /// Whether `slot` is still the registered slot for `dir`.
    fn is_registered(&self, dir: &Path, slot: &Slot) -> bool {
        self.map().get(dir).is_some_and(|s| Arc::ptr_eq(s, slot))
    }

    fn snapshot(&self) -> Vec<(PathBuf, Slot)> {
        self.map()
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

/// A lifecycle lease on a project's worker slot, taken **before** an op's
/// async capability probes (#1881). Retirement removes and marks retired
/// every registered slot, so a lease taken before a delete/rename is
/// invalidated by it; [`run_serve_op`] re-validates the lease before it
/// registers as a waiter or spawns, so a request resolved before retirement
/// can never start a worker afterwards.
///
/// The slot is keyed by the canonical directory, which several project
/// aliases may share; deleting one alias keeps the slot alive for the others.
/// A lease therefore also carries an optional, separately revocable *name
/// binding* ([`Lease::bound_to`]), re-checked at every point the lease is —
/// including after the async waits for the project's turn and admission — so
/// a deleted alias can never run an op through its sibling's slot.
pub(super) struct Lease {
    dir: PathBuf,
    slot: Slot,
    binding: Option<Box<dyn Fn() -> bool + Send + Sync>>,
}

impl Lease {
    /// Attach a name binding: the lease counts as revoked once `still_binds`
    /// returns `false`.
    pub(super) fn bound_to(
        mut self,
        still_binds: impl Fn() -> bool + Send + Sync + 'static,
    ) -> Self {
        self.binding = Some(Box::new(still_binds));
        self
    }

    /// Whether retirement (or loss of the name binding) has invalidated this
    /// lease.
    fn revoked(&self, registry: &WorkerRegistry) -> bool {
        self.slot.active().retired
            || !registry.is_registered(&self.dir, &self.slot)
            || self.binding.as_ref().is_some_and(|b| !b())
    }
}

/// Take a [`Lease`] on `dir`'s slot, or `None` while the path is being
/// deleted/renamed.
pub(super) fn lease(limiter: &DataGatewayLimiter, dir: &Path) -> Option<Lease> {
    let slot = limiter.workers.live_slot_for(dir)?;
    Some(Lease {
        dir: dir.to_path_buf(),
        slot,
        binding: None,
    })
}

/// Holds a project path tombstoned while its directory is deleted/renamed:
/// ops on it fail fast and spawn nothing. Dropping it lifts the tombstone.
#[must_use = "the tombstone lifts when this guard drops"]
pub(super) struct Retired<'a> {
    registry: &'a WorkerRegistry,
    keys: Vec<PathBuf>,
}

impl Drop for Retired<'_> {
    fn drop(&mut self) {
        let mut t = self.registry.tombstones();
        for k in &self.keys {
            if let Some(n) = t.get_mut(k) {
                *n -= 1;
                if *n == 0 {
                    t.remove(k);
                }
            }
        }
        // Forget retired slots that have drained (bounded bookkeeping).
        for k in &self.keys {
            self.registry.retiring_busy(k);
        }
    }
}

/// Kill a worker's process group and reap it from a blocking context,
/// holding a [`Reaping`] ticket and a [`Waiting`] place on `slot` meanwhile.
/// If it has not exited within the reap grace it is **not** treated as gone:
/// a background thread keeps both held and keeps killing/reaping until the
/// process exits, so a retirement drain on `slot` fails closed (#1836).
/// Returns whether the process exited within the grace.
fn kill_and_reap_blocking(limiter: &DataGatewayLimiter, slot: &Slot, w: Worker) -> bool {
    let ticket = Reaping::new(limiter);
    let hold = ReapHold::new(slot);
    let budget = limiter.reaping.clone();
    let mut child = w.child;
    let pid = w.pid;
    budget.kill(pid, &mut child);
    let give_up = Instant::now() + budget.grace();
    loop {
        if worker_gone(&budget, &mut child, pid) {
            return true;
        }
        if Instant::now() >= give_up {
            break;
        }
        // Re-kill: a descendant may outlive the reaped leader.
        budget.kill(pid, &mut child);
        std::thread::sleep(Duration::from_millis(10));
    }
    warn_unreaped(pid);
    std::thread::spawn(move || {
        let _ticket = ticket;
        let _hold = hold;
        let mut zombies = ZombieGrace::default();
        while !worker_gone(&budget, &mut child, pid) {
            budget.kill(pid, &mut child);
            let leader_reaped = child_exited(&mut child);
            if zombies.expired(&budget, pid, leader_reaped, Instant::now()) {
                warn_zombies(pid);
                return;
            }
            std::thread::sleep(REAP_RETRY);
        }
        tracing::info!(?pid, "data worker: unreaped process finally exited");
    });
    false
}

/// How long a group whose leader is reaped may keep answering the liveness
/// probe while it is re-sent SIGKILL. Past it, every member has had SIGKILL,
/// so what remains can only be unreaped zombies (a PID 1 that does not reap):
/// they hold no directory or budget, so the ticket and hold are released
/// rather than kept forever (#1836).
const ZOMBIE_GRACE: Duration = Duration::from_secs(5);

/// Bounds the alive-until-ESRCH rule once the leader is reaped: the clock
/// starts at the first kill after the leader is reaped (the group has had
/// SIGKILL) and [`ZombieGrace::expired`] reports when the grace passed.
/// Only a group our SIGKILL can reach ([`Liveness::Signalable`]) is ever
/// released: an EPERM group has a live member the kill could not touch, so
/// it is held (and retirement fails closed) for as long as it answers.
#[derive(Default)]
struct ZombieGrace {
    since: Option<Instant>,
    eperm_warned: bool,
}

impl ZombieGrace {
    /// Call after each group kill. The clock resets unless the leader is
    /// reaped and the group is signalable.
    fn expired(
        &mut self,
        budget: &ReapBudget,
        pid: Option<u32>,
        leader_reaped: bool,
        now: Instant,
    ) -> bool {
        let liveness = budget.liveness(pid);
        if liveness == Liveness::Eperm && !self.eperm_warned {
            self.eperm_warned = true;
            warn_eperm(pid);
        }
        if !leader_reaped || liveness != Liveness::Signalable {
            self.since = None;
            return false;
        }
        let since = *self.since.get_or_insert(now);
        now.saturating_duration_since(since) >= budget.zombie_grace()
    }
}

fn warn_eperm(pid: Option<u32>) {
    tracing::warn!(
        pgid = ?pid,
        "data worker group has a member SIGKILL cannot reach (EPERM); \
         holding its slot and budget, and project retirement fails closed"
    );
}

fn warn_zombies(pid: Option<u32>) {
    tracing::warn!(
        pgid = ?pid,
        "data worker group still probes alive after SIGKILL and the zombie grace; \
         assuming unreaped zombies and releasing its slot and budget"
    );
}

/// Interval between kill/reap attempts on a worker that outlived the grace.
const REAP_RETRY: Duration = Duration::from_millis(50);

fn child_exited(child: &mut Child) -> bool {
    matches!(child.try_wait(), Ok(Some(_)) | Err(_))
}

/// A worker counts as exited only when its leader is reaped **and** its
/// process group is empty: a launcher (pnpm/yarn shim, wrapper) may exit and
/// be reaped while forked descendants keep running (#1836).
fn worker_gone(budget: &ReapBudget, child: &mut Child, pid: Option<u32>) -> bool {
    child_exited(child) && budget.liveness(pid) == Liveness::Gone
}

/// Whether signalling the group led by `pid` cannot hit an unrelated group.
/// While the leader is unreaped its pid (and so the pgid) is held; once it is
/// reaped the pgid is only held while the group has members, so a reaped
/// leader's group is signalled only after probing it non-empty — never by a
/// stale pid the kernel may have reused (#1836).
fn group_signal_safe(child: &mut Child, pid: Option<u32>) -> bool {
    pid.is_some() && (matches!(child.try_wait(), Ok(None)) || group_alive(pid))
}

/// Result of probing a worker's process group.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Liveness {
    /// No member left (ESRCH).
    Gone,
    /// Members exist and our signals reach them: pending SIGKILL or zombies.
    Signalable,
    /// Members exist that we may not signal (EPERM): a live process our
    /// SIGKILL cannot reach. Never treated as gone or as zombies (#1836).
    Eperm,
}

/// Whether any process remains in the group led by `pid`.
fn group_alive(pid: Option<u32>) -> bool {
    group_liveness(pid) != Liveness::Gone
}

/// Probe the group led by `pid` (Unix). Windows has no group probe here (see
/// the Job Object follow-up), so it reports none.
fn group_liveness(pid: Option<u32>) -> Liveness {
    #[cfg(unix)]
    if let Some(pid) = pid.filter(|p| *p != 0) {
        let Ok(pgid) = i32::try_from(pid) else {
            return Liveness::Gone;
        };
        // SAFETY: signal 0 to a negative pid only probes the group.
        if unsafe { libc::kill(-pgid, 0) } == 0 {
            return Liveness::Signalable;
        }
        return match std::io::Error::last_os_error().raw_os_error() {
            Some(libc::ESRCH) => Liveness::Gone,
            Some(libc::EPERM) => Liveness::Eperm,
            // Unknown error: fail closed (never released as zombies).
            _ => Liveness::Eperm,
        };
    }
    let _ = pid;
    Liveness::Gone
}

fn warn_unreaped(pid: Option<u32>) {
    tracing::warn!(
        ?pid,
        "data worker did not exit after kill; holding its slot and budget until it does"
    );
}

/// Retire the project's worker(s) before its directory is deleted/renamed
/// (#1836): tombstone the path (new ops fail fast, nothing is spawned), kill
/// the idle worker and the **in-flight** one (whole process group), then wait
/// (bounded by [`RETIRE_DRAIN`]) until every op has left the slot, so the
/// directory operation sees no writer. Hold the returned guard for the length
/// of the directory operation. Blocks the calling thread — call it from a
/// blocking context. Fails closed (`WouldBlock`) if an op does not drain.
pub(super) fn retire_project_worker_blocking<'a>(
    limiter: &'a DataGatewayLimiter,
    dir: &Path,
) -> std::io::Result<Retired<'a>> {
    // Callers key on the canonical dir; accept either spelling here.
    let canon = dunce::canonicalize(dir).ok();
    let mut keys = vec![dir.to_path_buf()];
    keys.extend(canon.filter(|c| c != dir));
    let removed: Vec<Slot> = {
        let mut map = limiter.workers.map();
        let mut t = limiter.workers.tombstones();
        // An overlapping retirement may still be draining this path's
        // worker: fail closed rather than let the directory op race it.
        if keys
            .iter()
            .any(|k| t.contains_key(k) || limiter.workers.retiring_busy(k))
        {
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "project busy: another delete/rename of it is in progress",
            ));
        }
        for k in &keys {
            *t.entry(k.clone()).or_default() += 1;
        }
        let removed: Vec<Slot> = keys.iter().filter_map(|k| map.remove(k)).collect();
        // Keep them discoverable until they drain, whatever this call's
        // outcome: a retry or a new op must not race a surviving worker.
        if !removed.is_empty() {
            let mut r = limiter.workers.retiring();
            for k in &keys {
                r.entry(k.clone())
                    .or_default()
                    .extend(removed.iter().cloned());
            }
        }
        removed
    };
    let guard = Retired {
        registry: &limiter.workers,
        keys,
    };
    let mut idle_pids = Vec::new();
    for slot in &removed {
        {
            let mut a = slot.active();
            a.retired = true;
            if let Some(pid) = a.pid {
                // The in-flight op sees EOF, fails, and leaves the slot.
                // Signalled under the `active` lock: the op clears `pid`
                // under this lock before it can reap the leader, so the
                // leader is unreaped here and its pid (= pgid) cannot have
                // been reused by an unrelated group.
                limiter.reaping.kill_group(pid);
            }
        }
        let idle = slot.idle().take();
        if let Some(w) = idle {
            idle_pids.push(w.pid);
            // An unreaped worker keeps its place on `slot`: the drain below
            // then fails closed instead of handing over a live directory.
            kill_and_reap_blocking(limiter, slot, w);
        }
    }
    let give_up = Instant::now() + limiter.reaping.drain();
    while removed
        .iter()
        .any(|s| s.waiters.load(Ordering::Acquire) > 0)
    {
        if Instant::now() >= give_up {
            for pid in idle_pids.iter().copied() {
                if limiter.reaping.liveness(pid) == Liveness::Eperm {
                    warn_eperm(pid);
                }
            }
            return Err(std::io::Error::new(
                std::io::ErrorKind::WouldBlock,
                "project busy: a data operation did not stop in time",
            ));
        }
        std::thread::sleep(Duration::from_millis(10));
    }
    Ok(guard)
}

/// Live-process accounting for worker teardowns in flight: killed workers
/// whose kill/reap has not finished are still live processes, so they count
/// against the budget, and admission waits on `notify` for them to go.
#[derive(Default)]
pub(crate) struct ReapBudget {
    pub(super) count: AtomicUsize,
    pub(super) notify: tokio::sync::Notify,
    /// Test seam: kills are not delivered (a worker that ignores the kill).
    #[cfg(test)]
    ignore_kill: std::sync::atomic::AtomicBool,
    /// Test seam: short reap grace and retirement drain.
    #[cfg(test)]
    short_waits: std::sync::atomic::AtomicBool,
    /// Test seam: number of group signals sent.
    #[cfg(test)]
    pub(super) group_signals: AtomicUsize,
    /// Test seam: faked group probe (0 = real, 1 = Signalable, 2 = Eperm).
    #[cfg(test)]
    fake_liveness: std::sync::atomic::AtomicU8,
}

impl ReapBudget {
    /// Kill a process group (blocking `taskkill` on Windows).
    fn kill_group(&self, pid: u32) {
        #[cfg(test)]
        self.group_signals.fetch_add(1, Ordering::AcqRel);
        #[cfg(test)]
        if self.ignore_kill.load(Ordering::Acquire) {
            return;
        }
        super::kill_group_by_pid(pid);
    }

    /// Kill the worker's group and the child itself (blocking on Windows).
    fn kill(&self, pid: Option<u32>, child: &mut Child) {
        #[cfg(test)]
        if self.ignore_kill.load(Ordering::Acquire) {
            return;
        }
        if let Some(pid) = pid
            && group_signal_safe(child, Some(pid))
        {
            self.kill_group(pid);
        }
        let _ = child.start_kill();
    }

    /// Probe the worker's group (faked in tests).
    fn liveness(&self, pid: Option<u32>) -> Liveness {
        #[cfg(test)]
        match self.fake_liveness.load(Ordering::Acquire) {
            1 => return Liveness::Signalable,
            2 => return Liveness::Eperm,
            _ => {}
        }
        group_liveness(pid)
    }

    fn zombie_grace(&self) -> Duration {
        #[cfg(test)]
        if self.short_waits.load(Ordering::Acquire) {
            return Duration::from_millis(200);
        }
        ZOMBIE_GRACE
    }

    fn grace(&self) -> Duration {
        #[cfg(test)]
        if self.short_waits.load(Ordering::Acquire) {
            return Duration::from_millis(100);
        }
        super::GATEWAY_REAP_GRACE
    }

    fn drain(&self) -> Duration {
        #[cfg(test)]
        if self.short_waits.load(Ordering::Acquire) {
            return Duration::from_millis(300);
        }
        RETIRE_DRAIN
    }
}

/// Kill a worker's whole tree — by the pid recorded at spawn, whether or not
/// the leader has already been reaped — and wait (bounded by the reap grace)
/// for the leader to be reaped and the group to empty. Returns whether it did.
async fn kill_tree(budget: &Arc<ReapBudget>, child: &mut Child, pid: Option<u32>) -> bool {
    async fn kill_group(budget: &Arc<ReapBudget>, child: &mut Child, pid: Option<u32>) {
        if !group_signal_safe(child, pid) {
            return;
        }
        if let Some(pid) = pid {
            // Blocking on Windows (`taskkill` status); cheap signal on Unix.
            let b = budget.clone();
            let _ = tokio::task::spawn_blocking(move || b.kill_group(pid)).await;
        }
    }
    kill_group(budget, child, pid).await;
    #[cfg(test)]
    let skip = budget.ignore_kill.load(Ordering::Acquire);
    #[cfg(not(test))]
    let skip = false;
    if !skip {
        let _ = child.start_kill();
    }
    let give_up = tokio::time::Instant::now() + budget.grace();
    if tokio::time::timeout_at(give_up, child.wait())
        .await
        .is_err()
    {
        return false;
    }
    // Leader reaped: kill again for descendants that outlived it.
    loop {
        kill_group(budget, child, pid).await;
        if budget.liveness(pid) == Liveness::Gone {
            return true;
        }
        if tokio::time::Instant::now() >= give_up {
            return false;
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
}

/// Keep killing and reaping a worker that outlived its grace until it (and
/// its whole group) exits.
async fn reap_until_exit(budget: &Arc<ReapBudget>, child: &mut Child, pid: Option<u32>) {
    warn_unreaped(pid);
    let mut zombies = ZombieGrace::default();
    while !kill_tree(budget, child, pid).await {
        let leader_reaped = child_exited(child);
        if zombies.expired(budget, pid, leader_reaped, Instant::now()) {
            warn_zombies(pid);
            return;
        }
        tokio::time::sleep(REAP_RETRY).await;
    }
    tracing::info!(?pid, "data worker: unreaped process finally exited");
}

/// Counts one worker teardown against the live-process budget until dropped.
struct Reaping(Arc<ReapBudget>);

impl Reaping {
    fn new(limiter: &DataGatewayLimiter) -> Self {
        limiter.reaping.count.fetch_add(1, Ordering::AcqRel);
        Self(limiter.reaping.clone())
    }
}

impl Drop for Reaping {
    fn drop(&mut self) {
        self.0.count.fetch_sub(1, Ordering::AcqRel);
        self.0.notify.notify_waiters();
    }
}

/// Kill a worker's process group without blocking the calling (possibly
/// async, possibly `Drop`) context: the kill — a blocking `taskkill` on
/// Windows — and the reap run on a blocking thread / background task that
/// owns the `Child`. Until the reap completes the worker stays counted in
/// [`DataGatewayLimiter`]'s `reaping` budget, since it is still a live process,
/// and holds a [`Waiting`] place on its project's `slot`, so a retirement drain
/// (waiters==0) cannot finish while the worker may still hold the dir (#1836).
fn kill_detached(limiter: &DataGatewayLimiter, slot: &Slot, mut w: Worker) {
    let pid = w.pid;
    let ticket = Reaping::new(limiter);
    let hold = ReapHold::new(slot);
    // The Unix group signal is a cheap syscall: send it now so the worker
    // stops at once, whatever happens to the background task.
    #[cfg(unix)]
    if let Some(pid) = pid {
        limiter.reaping.kill_group(pid);
    }
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        // No runtime (shutdown): best effort, inline. `kill_on_drop` backs it.
        #[cfg(not(unix))]
        if let Some(pid) = pid {
            super::kill_group_by_pid(pid);
        }
        let _ = w.child.start_kill();
        drop(ticket);
        drop(hold);
        return;
    };
    let budget = limiter.reaping.clone();
    rt.spawn(async move {
        let _ticket = ticket;
        let _hold = hold;
        // Not reaped in time: still a live process — keep the ticket and the
        // slot place until it really exits.
        if !kill_tree(&budget, &mut w.child, pid).await {
            reap_until_exit(&budget, &mut w.child, pid).await;
        }
    });
}

/// Enforce the console-wide live-process budget (#1329). Called by every op
/// right after it is admitted (holding a permit) — and, for a serve op, after
/// it has taken its own worker out of the idle slot, so that worker is counted
/// by the op's permit rather than as idle. Idle workers are retired,
/// least-recently-used first, until
/// `idle resident + running ops + reaping <= concurrency`; any idle past
/// [`IDLE_TTL`] goes too. Idle workers are evicted regardless of whether an op
/// holds their project's turn, so a turn-holder waiting for a permit pins no
/// process.
pub(super) fn enforce_resident_budget(limiter: &DataGatewayLimiter) {
    let running = limiter
        .concurrency
        .saturating_sub(limiter.permits.available_permits());
    let reaping = limiter.reaping.count.load(Ordering::Acquire);
    let allowed_idle = limiter
        .concurrency
        .saturating_sub(running)
        .saturating_sub(reaping);
    let mut idle: Vec<(Instant, Slot)> = limiter
        .workers
        .snapshot()
        .into_iter()
        .filter_map(|(_, slot)| {
            let last = slot.idle().as_ref().map(|w| w.last_used);
            last.map(|l| (l, slot))
        })
        .collect();
    // Newest first; retire everything past the allowance or past the TTL.
    idle.sort_by_key(|g| std::cmp::Reverse(g.0));
    for (i, (last, slot)) in idle.into_iter().enumerate() {
        if i >= allowed_idle || last.elapsed() >= IDLE_TTL {
            // Keep the idle mutex held through `kill_detached`, so its
            // `ReapHold` is registered before any retirement can observe the
            // slot empty with no waiters while the process is still alive.
            let mut guard = slot.idle();
            if let Some(w) = guard.take() {
                tracing::debug!(pid = ?w.pid, "data worker evicted (resident budget)");
                kill_detached(limiter, &slot, w);
            }
            drop(guard);
        }
    }
}

/// Idle resident workers: live processes not counted by any permit.
pub(super) fn idle_resident_count(limiter: &DataGatewayLimiter) -> usize {
    limiter
        .workers
        .snapshot()
        .into_iter()
        .filter(|(_, slot)| slot.idle().is_some())
        .count()
}

/// Identity of the toolkit + manifest + project directory a worker was started
/// against. Any change yields a different value and restarts the worker, so it
/// never serves a stale toolkit or datasource declaration.
fn toolkit_fingerprint(urban: &Path, dir: &Path) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    crate::urban::toolkit_identity(urban).hash(&mut h);
    std::fs::read(dir.join("nano.app.json")).ok().hash(&mut h);
    // The directory's own identity: a project deleted and recreated at the
    // same path is a different directory, so its worker (whose CWD is the
    // deleted one) must not be reused.
    if let Ok(md) = std::fs::metadata(dir) {
        md.created().ok().hash(&mut h);
        #[cfg(unix)]
        {
            use std::os::unix::fs::MetadataExt;
            (md.dev(), md.ino()).hash(&mut h);
        }
    }
    h.finish()
}

fn spawn_worker(urban: &Path, dir: &Path, fingerprint: u64) -> Result<Worker, DataError> {
    let mut cmd = Command::new(urban);
    cmd.current_dir(dir)
        .args(["data", "--serve"])
        .env("NO_COLOR", "1")
        .stdin(std::process::Stdio::piped())
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .kill_on_drop(true);
    // Lead a fresh process group so a kill reaps the wrapper *and* its Node
    // worker (#1329).
    #[cfg(unix)]
    cmd.process_group(0);
    let mut child = cmd
        .spawn()
        .map_err(|e| DataError::Gateway(format!("spawn data worker: {e}")))?;
    let missing = || DataError::Gateway("data worker: stdio not piped".to_string());
    let stdin = child.stdin.take().ok_or_else(missing)?;
    let stdout = child.stdout.take().ok_or_else(missing)?;
    let stderr = child.stderr.take().ok_or_else(missing)?;
    let stderr_tail = Arc::new(Mutex::new(String::new()));
    // Drain stderr continuously (a full pipe would wedge the worker) and keep
    // a bounded tail for crash diagnostics.
    let tail = stderr_tail.clone();
    tokio::spawn(async move {
        // Fixed-size chunks, never whole lines: a newline-free flood cannot
        // grow console memory.
        let mut stderr = stderr;
        let mut buf = [0u8; 4096];
        while let Ok(n) = stderr.read(&mut buf).await {
            if n == 0 {
                break;
            }
            let chunk = String::from_utf8_lossy(&buf[..n]);
            tracing::debug!(target: "data_worker", "{chunk}");
            let mut t = tail.lock().unwrap_or_else(|e| e.into_inner());
            t.push_str(&chunk);
            if t.len() > STDERR_TAIL_BYTES {
                let mut cut = t.len() - STDERR_TAIL_BYTES;
                while !t.is_char_boundary(cut) {
                    cut += 1;
                }
                t.drain(..cut);
            }
        }
    });
    tracing::debug!(dir = %dir.display(), pid = ?child.id(), "data worker started");
    Ok(Worker {
        pid: child.id(),
        child,
        stdin,
        stdout: BufReader::new(stdout),
        fingerprint,
        next_id: 1,
        in_flight: false,
        last_used: Instant::now(),
        stderr_tail,
    })
}

/// Test seam type: see `DataGatewayLimiter::after_spawn`.
#[cfg(test)]
pub(super) type AfterSpawnHook = Box<dyn FnOnce(Option<u32>) + Send>;

/// Run a worker teardown on its own task that owns the worker, a [`Reaping`]
/// ticket (live-process budget) and a [`Waiting`] hold on the slot (so a
/// retirement drain waits for it), then await it. If the caller is cancelled
/// mid-teardown the task still finishes the kill/reap: ownership never falls
/// back to a bare `Child` drop that skips the process-group cleanup (#1836).
/// The caller waits only for the bounded kill/reap; a process that outlives
/// it stays owned by the task, which keeps the ticket and the slot place
/// until it exits. Returns whether the process exited within the grace.
async fn teardown_owned(
    limiter: &DataGatewayLimiter,
    slot: &Slot,
    w: Worker,
    graceful: bool,
) -> bool {
    let ticket = Reaping::new(limiter);
    let hold = ReapHold::new(slot);
    let budget = limiter.reaping.clone();
    let (tx, rx) = tokio::sync::oneshot::channel();
    tokio::spawn(async move {
        let _ticket = ticket;
        let _hold = hold;
        let Worker {
            mut child,
            stdin,
            pid,
            ..
        } = w;
        let exited = if graceful {
            retire_child(&budget, &mut child, stdin, pid).await
        } else {
            drop(stdin);
            kill_tree(&budget, &mut child, pid).await
        };
        let _ = tx.send(exited);
        if !exited {
            reap_until_exit(&budget, &mut child, pid).await;
        }
    });
    rx.await.unwrap_or(false)
}

/// Retire a healthy worker gracefully: close stdin (serve mode exits 0 on EOF)
/// and kill it only if it doesn't go within [`RETIRE_GRACE`]. Returns whether
/// it exited.
async fn retire_child(
    budget: &Arc<ReapBudget>,
    child: &mut Child,
    stdin: ChildStdin,
    pid: Option<u32>,
) -> bool {
    drop(stdin);
    if tokio::time::timeout(RETIRE_GRACE, child.wait())
        .await
        .is_ok()
        && budget.liveness(pid) == Liveness::Gone
    {
        return true;
    }
    // Hung, or a clean leader exit that left descendants: kill the group.
    kill_tree(budget, child, pid).await
}

fn with_stderr(w: &Worker, msg: String) -> DataError {
    let tail = w
        .stderr_tail
        .lock()
        .unwrap_or_else(|e| e.into_inner())
        .trim()
        .to_string();
    DataError::Gateway(if tail.is_empty() {
        msg
    } else {
        format!("{msg}: {tail}")
    })
}

/// Write one request line and read until the reply echoing its `id`.
async fn exchange(
    w: &mut Worker,
    id: u64,
    request: &serde_json::Value,
) -> Result<serde_json::Value, DataError> {
    let mut req = request.clone();
    let obj = req
        .as_object_mut()
        .ok_or_else(|| DataError::Gateway("data request must be a JSON object".to_string()))?;
    obj.insert("id".to_string(), serde_json::json!(id));
    let mut line = serde_json::to_vec(&req)
        .map_err(|e| DataError::Gateway(format!("serialize request: {e}")))?;
    line.push(b'\n');
    w.in_flight = true;
    if let Err(e) = w.stdin.write_all(&line).await {
        return Err(with_stderr(w, format!("data worker write failed: {e}")));
    }
    if let Err(e) = w.stdin.flush().await {
        return Err(with_stderr(w, format!("data worker write failed: {e}")));
    }
    let want = serde_json::json!(id);
    loop {
        let mut raw = Vec::new();
        let n = match (&mut w.stdout)
            .take(MAX_REPLY_LINE_BYTES as u64 + 1)
            .read_until(b'\n', &mut raw)
            .await
        {
            Ok(n) => n,
            Err(e) => return Err(with_stderr(w, format!("data worker read failed: {e}"))),
        };
        if n > MAX_REPLY_LINE_BYTES {
            // The caller kills a worker whose exchange failed.
            return Err(with_stderr(
                w,
                format!("data worker reply line exceeds {MAX_REPLY_LINE_BYTES} bytes"),
            ));
        }
        let buf = String::from_utf8_lossy(&raw);
        if n == 0 {
            let status = w.child.wait().await.ok();
            let msg = match status {
                Some(s) => format!("data worker exited ({s}) before replying"),
                None => "data worker exited before replying".to_string(),
            };
            return Err(with_stderr(w, msg));
        }
        let text = buf.trim();
        if text.is_empty() {
            continue;
        }
        match serde_json::from_str::<serde_json::Value>(text) {
            // Our reply, or the worker's `{id:null, ok:false}` answer to a line
            // it could not parse. Only ever ours because the project turn keeps
            // exactly one request in flight per worker — accepting `id:null`
            // relies on that invariant.
            Ok(v)
                if v.get("id") == Some(&want) || v.get("id") == Some(&serde_json::Value::Null) =>
            {
                w.in_flight = false;
                return Ok(v);
            }
            _ => {
                tracing::debug!(
                    target: "data_worker",
                    line = %super::gateway_output_snippet(text),
                    "ignoring non-reply stdout line"
                );
            }
        }
    }
}

/// Held by a teardown until its process has exited: keeps a waiter place on
/// the slot (a retirement drain waits) and blocks a replacement spawn.
struct ReapHold(Slot);
impl ReapHold {
    fn new(slot: &Slot) -> Self {
        slot.waiters.fetch_add(1, Ordering::AcqRel);
        slot.unreaped.fetch_add(1, Ordering::AcqRel);
        Self(slot.clone())
    }
}
impl Drop for ReapHold {
    fn drop(&mut self) {
        self.0.unreaped.fetch_sub(1, Ordering::AcqRel);
        self.0.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Wait (until `deadline`) for every teardown of `slot`'s workers to see its
/// process exit, so a replacement never runs beside a surviving predecessor.
async fn await_predecessors_reaped(
    limiter: &DataGatewayLimiter,
    slot: &Slot,
    deadline: tokio::time::Instant,
) -> Result<(), DataError> {
    while slot.unreaped.load(Ordering::Acquire) > 0 {
        if tokio::time::Instant::now() >= deadline {
            return Err(DataError::Gateway(format!(
                "data gateway timed out after {:?}: previous data worker has not exited",
                limiter.timeout
            )));
        }
        tokio::time::sleep(Duration::from_millis(10)).await;
    }
    Ok(())
}

/// Decrements a slot's waiter count when the op leaves (even if cancelled).
struct Waiting(Slot);
impl Drop for Waiting {
    fn drop(&mut self) {
        self.0.waiters.fetch_sub(1, Ordering::AcqRel);
    }
}

/// Owns the worker while an op uses it: if the op's future is dropped
/// (caller cancelled) mid-exchange, the worker is killed at once without
/// blocking the dropping thread ([`kill_detached`]) — no later request is
/// needed to stop a hung or abandoned operation.
struct InUse<'a> {
    limiter: &'a DataGatewayLimiter,
    slot: Slot,
    worker: Option<Worker>,
}

impl Drop for InUse<'_> {
    fn drop(&mut self) {
        self.slot.active().pid = None;
        if let Some(w) = self.worker.take() {
            kill_detached(self.limiter, &self.slot, w);
        }
    }
}

fn timed_out(limiter: &DataGatewayLimiter) -> DataError {
    DataError::Gateway(format!(
        "data gateway timed out after {:?}",
        limiter.timeout
    ))
}

/// Run one datasource op through the project's long-lived `urban data --serve`
/// worker, spawning/restarting it as needed. Ops for one project queue on its
/// worker (bounded by the limiter's queue size, without holding an execution
/// permit); admission then goes through the console-wide
/// [`DataGatewayLimiter`] (Busy back-pressure, #1329). One deadline (the
/// limiter's timeout) covers the project wait, admission and the exchange.
pub(super) async fn run_serve_op(
    limiter: &DataGatewayLimiter,
    urban: &Path,
    lease: Lease,
    request: &serde_json::Value,
) -> Result<serde_json::Value, DataError> {
    let deadline = tokio::time::Instant::now() + limiter.timeout;
    let retired = || DataError::NoProject;
    let dir = lease.dir.as_path();
    let slot = lease.slot.clone();
    // Bound the per-project queue: at most one holder + `queue` waiters.
    let prev = slot.waiters.fetch_add(1, Ordering::AcqRel);
    let _waiting = Waiting(slot.clone());
    // Re-validate the lease only after registering as a waiter: a retirement
    // that has not yet marked the slot retired will then see (and drain) us;
    // one that already has is seen here (#1881).
    if lease.revoked(&limiter.workers) {
        return Err(retired());
    }
    if prev > limiter.queue {
        return Err(limiter.busy());
    }
    // An op that must wait for its project's turn occupies a place in the
    // console-wide queue for the whole wait (and keeps it through admission,
    // counted once), so blocked projects cannot multiply the #1329 bound.
    let (_turn, ticket) = match slot.turn.try_lock() {
        Ok(t) => (t, None),
        Err(_) => {
            let ticket = limiter.reserve_queue()?;
            let t = tokio::time::timeout_at(deadline, slot.turn.lock())
                .await
                .map_err(|_| timed_out(limiter))?;
            (t, Some(ticket))
        }
    };

    let _permit = tokio::time::timeout_at(deadline, limiter.acquire_with(ticket))
        .await
        .map_err(|_| timed_out(limiter))??;
    // Take our worker first: from here it is counted by our permit. The
    // turn/admission waits above are async, so re-validate the whole lease
    // (retirement *and* name binding, #1881) before touching the worker.
    if lease.revoked(&limiter.workers) {
        return Err(retired());
    }
    let mut use_ = InUse {
        limiter,
        slot: slot.clone(),
        worker: slot.idle().take(),
    };
    enforce_resident_budget(limiter);
    tokio::time::timeout_at(deadline, limiter.await_teardown_capacity())
        .await
        .map_err(|_| timed_out(limiter))?;
    let fingerprint = toolkit_fingerprint(urban, dir);

    if let Some(mut w) = use_.worker.take() {
        let alive = matches!(w.child.try_wait(), Ok(None));
        if !alive || w.in_flight {
            tracing::debug!(dir = %dir.display(), alive, in_flight = w.in_flight, "data worker restart");
            teardown_owned(limiter, &slot, w, false).await;
        } else if w.fingerprint != fingerprint {
            tracing::debug!(dir = %dir.display(), "data worker restart: toolkit/manifest changed");
            teardown_owned(limiter, &slot, w, true).await;
        } else {
            use_.worker = Some(w);
        }
    }
    if use_.worker.is_none() {
        // A predecessor that outlived its kill grace still owns the project
        // dir: no replacement until it is really gone.
        await_predecessors_reaped(limiter, &slot, deadline).await?;
        use_.worker = Some(spawn_worker(urban, dir, fingerprint)?);
        #[cfg(test)]
        {
            let hook = limiter
                .after_spawn
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .take();
            if let Some(h) = hook {
                h(use_.worker.as_ref().and_then(|w| w.pid));
            }
        }
    }
    {
        // Publish the in-use pid so retirement can kill it; if the project
        // was retired meanwhile, this worker must not run: reap it before
        // returning (and before `_waiting` drops), so a drain that sees
        // waiters==0 never races a still-live worker holding the project dir.
        // The teardown/reap waits above are async too: a lost name binding
        // fails the op here, before any request reaches the worker (#1881).
        // The worker itself stays: it serves the surviving sibling aliases.
        let unbound = lease.binding.as_ref().is_some_and(|b| !b());
        let retired_now = {
            let mut a = slot.active();
            if !a.retired {
                if unbound {
                    if let Some(w) = use_.worker.take() {
                        if limiter.workers.is_registered(dir, &slot) {
                            *slot.idle() = Some(w);
                        } else {
                            use_.worker = Some(w);
                        }
                    }
                } else {
                    a.pid = use_.worker.as_ref().and_then(|w| w.pid);
                }
            }
            a.retired || unbound
        };
        if retired_now {
            if let Some(w) = use_.worker.take() {
                teardown_owned(limiter, &slot, w, false).await;
            }
            return Err(retired());
        }
    }
    let result = match use_.worker.as_mut() {
        Some(w) => {
            let id = w.next_id;
            w.next_id += 1;
            w.last_used = Instant::now();
            let r = tokio::time::timeout_at(deadline, exchange(w, id, request)).await;
            w.last_used = Instant::now();
            r
        }
        None => Ok(Err(DataError::Gateway(
            "internal error: data worker slot empty".to_string(),
        ))),
    };
    let ok = matches!(result, Ok(Ok(_)));
    if let Some(w) = use_.worker.take() {
        // Parked under `active`, so retirement cannot miss it.
        let reject = {
            let mut a = slot.active();
            a.pid = None;
            if ok && !a.retired && limiter.workers.is_registered(dir, &slot) {
                *slot.idle() = Some(w);
                None
            } else {
                Some(w)
            }
        };
        if let Some(w) = reject {
            // Failed/timed-out (possibly desynced) worker, or one whose
            // project was retired while this op ran: never park it.
            teardown_owned(limiter, &slot, w, false).await;
        }
    }
    match result {
        Ok(Ok(mut reply)) => {
            if let Some(obj) = reply.as_object_mut() {
                obj.remove("id");
            }
            super::interpret_gateway_reply(reply)
        }
        Ok(Err(e)) => Err(e),
        Err(_) => Err(timed_out(limiter)),
    }
}

#[cfg(all(test, unix))]
#[allow(clippy::unwrap_used, clippy::expect_used, reason = "test helpers")]
mod tests {
    use super::*;

    /// Lease-then-run, as `run_data_op_inner` does (minus the probes).
    async fn run_serve_op(
        limiter: &DataGatewayLimiter,
        urban: &Path,
        dir: &Path,
        request: &serde_json::Value,
    ) -> Result<serde_json::Value, DataError> {
        let lease = lease(limiter, dir).ok_or(DataError::NoProject)?;
        super::run_serve_op(limiter, urban, lease, request).await
    }

    /// #1881: a lease taken before retirement (i.e. before the op's async
    /// capability probes finished) is revoked by it — the op fails closed and
    /// never spawns a worker, even after the tombstone has lifted — while a
    /// fresh lease taken afterwards works.
    #[tokio::test]
    async fn lease_taken_before_retirement_never_spawns() {
        let dir = project("lease");
        let spawns = dir.join("spawns.txt");
        let stub = serve_stub(&dir, &spawns, 0);
        let lim = limiter();
        let stale = lease(&lim, &dir).unwrap();
        drop(retire_project_worker_blocking(&lim, &dir).unwrap());
        let r = super::run_serve_op(&lim, &stub, stale, &serde_json::json!({"op":"schema"})).await;
        assert!(matches!(r, Err(DataError::NoProject)), "{r:?}");
        assert_eq!(spawn_count(&spawns), 0, "a revoked lease spawned a worker");
        assert!(
            !lim.workers.map().contains_key(&dir),
            "a revoked lease re-registered a slot"
        );
        run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        assert_eq!(spawn_count(&spawns), 1);
        drop(retire_project_worker_blocking(&lim, &dir).unwrap());
    }

    /// #1881: an alias deleted while its op waits for the project's turn
    /// (a sibling alias keeps the shared slot alive) fails closed after the
    /// wait, sends nothing, and leaves the sibling's warm worker running.
    #[tokio::test]
    async fn name_binding_lost_during_admission_fails_closed_and_keeps_worker() {
        use std::sync::atomic::AtomicBool;
        let dir = project("binding");
        let spawns = dir.join("spawns.txt");
        let stub = serve_stub(&dir, &spawns, 0);
        let lim = limiter();
        // A sibling alias warms the shared worker.
        run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        assert_eq!(spawn_count(&spawns), 1);
        let bound = Arc::new(AtomicBool::new(true));
        let b = bound.clone();
        let l = lease(&lim, &dir)
            .unwrap()
            .bound_to(move || b.load(Ordering::SeqCst));
        let slot = l.slot.clone();
        let turn = slot.turn.lock().await;
        let req = serde_json::json!({"op":"schema"});
        let r = {
            let op = super::run_serve_op(&lim, &stub, l, &req);
            tokio::pin!(op);
            // Park the op behind the held turn, then delete the alias.
            assert!(
                tokio::time::timeout(std::time::Duration::from_millis(100), &mut op)
                    .await
                    .is_err()
            );
            bound.store(false, Ordering::SeqCst);
            drop(turn);
            op.await
        };
        assert!(matches!(r, Err(DataError::NoProject)), "{r:?}");
        assert!(slot.idle().is_some(), "the sibling's worker was torn down");
        run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        assert_eq!(spawn_count(&spawns), 1, "the sibling had to respawn");
        drop(retire_project_worker_blocking(&lim, &dir).unwrap());
    }

    fn write_stub(path: &Path, body: &str) {
        use std::os::unix::fs::PermissionsExt;
        std::fs::write(path, body).unwrap();
        std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o755)).unwrap();
    }

    /// A serve-mode stub `urban`: every invocation appends a line to `spawns`
    /// (so a test can count process starts), then answers each NDJSON request
    /// line with `{id, ok:true, pid, n}` where `n` counts requests served by
    /// this process. `crash_after` (if > 0) makes it exit after that many.
    fn serve_stub(dir: &Path, spawns: &Path, crash_after: u32) -> PathBuf {
        let stub = dir.join("urban-serve-stub.sh");
        write_stub(
            &stub,
            &format!(
                "#!/bin/sh\necho \"$*\" >> '{spawns}'\n\
                 n=0\n\
                 while IFS= read -r line; do\n\
                   n=$((n+1))\n\
                   id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9]*\\).*/\\1/p')\n\
                   echo 'log line on stdout'\n\
                   printf '{{\"id\":%s,\"ok\":true,\"pid\":%s,\"n\":%s}}\\n' \"$id\" \"$$\" \"$n\"\n\
                   if [ {crash_after} -gt 0 ] && [ $n -ge {crash_after} ]; then echo boom >&2; exit 3; fi\n\
                 done\n",
                spawns = spawns.display()
            ),
        );
        stub
    }

    /// A worker that survives stdin EOF and ignores SIGTERM: only a real
    /// SIGKILL stops it, so a test with `ignore_kill` set genuinely exercises
    /// a worker that outlives teardown.
    fn stubborn_stub(dir: &Path) -> PathBuf {
        let stub = dir.join("urban-stubborn-stub.sh");
        write_stub(
            &stub,
            "#!/bin/sh\ntrap '' TERM HUP\nwhile IFS= read -r line; do :; done\nwhile :; do sleep 0.05; done\n",
        );
        stub
    }

    fn spawn_count(spawns: &Path) -> usize {
        std::fs::read_to_string(spawns)
            .map(|s| s.lines().count())
            .unwrap_or(0)
    }

    fn limiter() -> DataGatewayLimiter {
        DataGatewayLimiter::with_timeout(2, 8, std::time::Duration::from_secs(10))
    }

    fn project(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "nbpm-data-worker-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("nano.app.json"), r#"{"schemaVersion":1}"#).unwrap();
        dir
    }

    /// #1836 acceptance: once the worker is warm, no data request starts a new
    /// process — every op is answered by the same long-lived worker.
    #[tokio::test]
    async fn warm_worker_serves_every_op_without_spawning() {
        let dir = project("warm");
        let spawns = dir.join("spawns.txt");
        let stub = serve_stub(&dir, &spawns, 0);
        let lim = limiter();
        let first = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        assert_eq!(spawn_count(&spawns), 1);
        assert_eq!(
            std::fs::read_to_string(&spawns).unwrap().trim(),
            "data --serve"
        );
        for i in 2..=10u64 {
            let v = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"sources"}))
                .await
                .unwrap();
            assert_eq!(v["pid"], first["pid"], "op served by a different process");
            assert_eq!(v["n"], serde_json::json!(i));
            assert!(v.get("id").is_none() && v.get("ok").is_none(), "{v:?}");
        }
        assert_eq!(spawn_count(&spawns), 1, "a warm op spawned a process");
    }

    /// A crashed worker is restarted on the next op.
    #[tokio::test]
    async fn crashed_worker_is_restarted() {
        let dir = project("crash");
        let spawns = dir.join("spawns.txt");
        let stub = serve_stub(&dir, &spawns, 1);
        let lim = limiter();
        let a = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        // Wait (bounded) until the crashed worker has actually exited, so this
        // exercises the between-ops crash deterministically rather than racing
        // the stub's exit (a crash racing an op is the mid-op case below).
        let pid = a["pid"].as_i64().unwrap() as i32;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        // SAFETY: signal 0 only probes for existence. The zombie (unreaped)
        // state still answers 0, so also accept the stderr marker it wrote
        // right before exiting as proof it is past its last reply.
        while unsafe { libc::kill(pid, 0) } == 0
            && !stderr_seen(&lim.workers.slot_for(&dir)).await
            && std::time::Instant::now() < deadline
        {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        let b = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        assert_ne!(a["pid"], b["pid"]);
        assert_eq!(spawn_count(&spawns), 2);
    }

    async fn stderr_seen(slot: &Slot) -> bool {
        let mut g = slot.idle();
        let Some(w) = g.as_mut() else { return false };
        let exited = matches!(w.child.try_wait(), Ok(Some(_)));
        exited
    }

    /// A worker dying mid-op surfaces a Gateway error carrying its stderr, and
    /// the next op gets a fresh worker.
    #[tokio::test]
    async fn worker_dying_mid_op_errors_then_recovers() {
        let dir = project("die");
        let spawns = dir.join("spawns.txt");
        let stub = dir.join("urban-die-stub.sh");
        write_stub(
            &stub,
            &format!(
                "#!/bin/sh\necho x >> '{}'\nread -r line\necho 'kaboom' >&2\nexit 7\n",
                spawns.display()
            ),
        );
        let lim = limiter();
        let err = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap_err();
        match err {
            DataError::Gateway(m) => assert!(m.contains("exited"), "{m}"),
            other => panic!("unexpected {other:?}"),
        }
        let _ = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"})).await;
        assert_eq!(spawn_count(&spawns), 2);
    }

    /// #1836: a second delete/rename of a path whose retirement is still
    /// in progress fails closed instead of reaching the filesystem while the
    /// first one's worker may still be draining.
    #[tokio::test]
    async fn overlapping_retirement_fails_closed() {
        let dir = project("retire-overlap");
        let lim = limiter();
        let first = retire_project_worker_blocking(&lim, &dir).unwrap();
        let second = retire_project_worker_blocking(&lim, &dir);
        assert!(
            matches!(&second, Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
            "overlapping retire must be rejected"
        );
        drop(second);
        drop(first);
        assert!(lim.workers.tombstones().is_empty(), "tombstone leaked");
        drop(retire_project_worker_blocking(&lim, &dir).unwrap());
    }

    /// #1836: cancelling the caller mid-teardown must not orphan the worker:
    /// the owned teardown task still kills and reaps it, keeping its budget
    /// ticket and slot hold (so a retirement drain waits) until done.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cancelled_teardown_still_reaps_worker() {
        let dir = project("teardown-cancel");
        let spawns = dir.join("spawns.txt");
        let stub = serve_stub(&dir, &spawns, 0);
        let lim = limiter();
        let w = spawn_worker(&stub, &dir, 0).unwrap();
        let pid = w.child.id().unwrap() as i32;
        let slot = lim.workers.slot_for(&dir);
        // Poll the teardown exactly once (it hands the worker to its task),
        // then drop it: the caller is cancelled before the teardown ends.
        let mut fut = Box::pin(teardown_owned(&lim, &slot, w, true));
        let waker = std::task::Waker::noop();
        let _ = fut
            .as_mut()
            .poll(&mut std::task::Context::from_waker(waker));
        drop(fut);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        // SAFETY: signal 0 only probes for existence (a reaped pid answers -1).
        while unsafe { libc::kill(pid, 0) } == 0
            || lim.reaping.count.load(Ordering::Acquire) != 0
            || slot.waiters.load(Ordering::Acquire) != 0
        {
            assert!(
                std::time::Instant::now() < deadline,
                "cancelled teardown orphaned the worker"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// #1836: a worker that does not die within the reap grace is not treated
    /// as gone. Retirement fails closed (WouldBlock, directory untouched) and
    /// the live-process budget and slot place stay held until it exits.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreaped_idle_worker_blocks_retirement_and_holds_budget() {
        let dir = project("unreaped-idle");
        let spawns = dir.join("spawns.txt");
        let stub = stubborn_stub(&dir);
        let serve = serve_stub(&dir, &spawns, 0);
        let lim = limiter();
        lim.reaping.ignore_kill.store(true, Ordering::Release);
        lim.reaping.short_waits.store(true, Ordering::Release);
        let w = spawn_worker(&stub, &dir, 0).unwrap();
        let pid = w.child.id().unwrap() as i32;
        let slot = lim.workers.slot_for(&dir);
        *slot.idle() = Some(w);
        let r = retire_project_worker_blocking(&lim, &dir);
        assert!(
            matches!(&r, Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
            "retire must fail closed over a live worker"
        );
        drop(r);
        assert!(dir.join("nano.app.json").exists(), "directory touched");
        // SAFETY: signal 0 only probes for existence.
        assert_eq!(unsafe { libc::kill(pid, 0) }, 0, "worker should be alive");
        assert_eq!(lim.reaping.count.load(Ordering::Acquire), 1);
        assert_eq!(slot.waiters.load(Ordering::Acquire), 1);
        // The failed retirement lifted its tombstone, but the surviving
        // worker keeps the path unavailable: a retry fails closed and a new
        // op neither gets a slot nor spawns a replacement.
        let retry = retire_project_worker_blocking(&lim, &dir);
        assert!(
            matches!(&retry, Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
            "retry must fail closed while the worker lives"
        );
        drop(retry);
        let op = run_serve_op(&lim, &serve, &dir, &serde_json::json!({"op":"schema"})).await;
        assert!(matches!(op, Err(DataError::NoProject)), "{op:?}");
        assert_eq!(
            spawn_count(&spawns),
            0,
            "replacement spawned beside survivor"
        );
        lim.reaping.ignore_kill.store(false, Ordering::Release);
        wait_released(&lim, &slot, pid).await;
        // Once it is gone the path is usable again.
        drop(retire_project_worker_blocking(&lim, &dir).unwrap());
        run_serve_op(&lim, &serve, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
    }

    /// #1836: restarting a project's worker never spawns the replacement while
    /// the predecessor outlives its kill: the op waits (within its deadline)
    /// until the predecessor is really gone, and only then spawns.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn restart_waits_for_surviving_predecessor() {
        let dir = project("restart-survivor");
        let spawns = dir.join("spawns.txt");
        let serve = serve_stub(&dir, &spawns, 0);
        let lim = Arc::new(limiter());
        lim.reaping.ignore_kill.store(true, Ordering::Release);
        lim.reaping.short_waits.store(true, Ordering::Release);
        let mut w = spawn_worker(&stubborn_stub(&dir), &dir, 0).unwrap();
        w.in_flight = true; // forces a restart on the next op
        let pid = w.child.id().unwrap() as i32;
        let slot = lim.workers.slot_for(&dir);
        *slot.idle() = Some(w);
        let survivor_alive_at_spawn = Arc::new(std::sync::atomic::AtomicBool::new(true));
        let seen = survivor_alive_at_spawn.clone();
        *lim.after_spawn.lock().unwrap() = Some(Box::new(move |_| {
            // SAFETY: signal 0 only probes for existence (a reaped pid answers -1).
            seen.store(unsafe { libc::kill(pid, 0) } == 0, Ordering::Release);
        }));
        let l2 = lim.clone();
        let sp = spawns.clone();
        let release = tokio::spawn(async move {
            // Well past the (short) kill grace: the op is now waiting.
            tokio::time::sleep(std::time::Duration::from_millis(800)).await;
            assert_eq!(spawn_count(&sp), 0, "spawned before predecessor exited");
            l2.reaping.ignore_kill.store(false, Ordering::Release);
        });
        let op = run_serve_op(&lim, &serve, &dir, &serde_json::json!({"op":"schema"})).await;
        release.await.unwrap();
        op.unwrap();
        assert!(
            !survivor_alive_at_spawn.load(Ordering::Acquire),
            "replacement spawned beside a surviving predecessor"
        );
        assert_eq!(spawn_count(&spawns), 1);
    }

    /// #1836: the async teardown path keeps an unreaped worker's budget
    /// ticket and slot place until it exits, while its caller moves on.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn unreaped_teardown_holds_budget_until_exit() {
        let dir = project("unreaped-teardown");
        let stub = stubborn_stub(&dir);
        let lim = limiter();
        lim.reaping.ignore_kill.store(true, Ordering::Release);
        lim.reaping.short_waits.store(true, Ordering::Release);
        let w = spawn_worker(&stub, &dir, 0).unwrap();
        let pid = w.child.id().unwrap() as i32;
        let slot = lim.workers.slot_for(&dir);
        let exited = teardown_owned(&lim, &slot, w, false).await;
        assert!(!exited, "a surviving worker reported as reaped");
        assert_eq!(lim.reaping.count.load(Ordering::Acquire), 1);
        assert_eq!(slot.waiters.load(Ordering::Acquire), 1);
        lim.reaping.ignore_kill.store(false, Ordering::Release);
        wait_released(&lim, &slot, pid).await;
    }

    /// #1836: a launcher that forks a descendant into its group and exits
    /// leaves the leader reaped (`Child::id()` is then `None`) while the
    /// descendant lives on. Teardown must still kill the whole group by the
    /// pid recorded at spawn, and hold the budget ticket and slot place until
    /// the group is empty — not just until the leader is reaped.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exited_launcher_descendants_are_killed_and_hold_budget() {
        let dir = project("orphan-descendant");
        let pidfile = dir.join("sleeper.pid");
        let stub = dir.join("urban-forking-stub.sh");
        write_stub(
            &stub,
            &format!(
                "#!/bin/sh\nsleep 1000 </dev/null >/dev/null 2>&1 &\necho $! > '{}'\nexit 0\n",
                pidfile.display()
            ),
        );
        let lim = limiter();
        lim.reaping.short_waits.store(true, Ordering::Release);
        let mut w = spawn_worker(&stub, &dir, 0).unwrap();
        // The restart path: the leader is found dead (and reaped) first.
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !matches!(w.child.try_wait(), Ok(Some(_))) {
            assert!(
                std::time::Instant::now() < deadline,
                "launcher never exited"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert!(w.child.id().is_none(), "leader should be reaped");
        let sleeper: i32 = std::fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // SAFETY: signal 0 only probes for existence.
        assert_eq!(unsafe { libc::kill(sleeper, 0) }, 0, "sleeper not running");
        let slot = lim.workers.slot_for(&dir);
        // Kills not delivered: the surviving descendant keeps the ticket held.
        lim.reaping.ignore_kill.store(true, Ordering::Release);
        let exited = teardown_owned(&lim, &slot, w, false).await;
        assert!(!exited, "a group with a live descendant reported as reaped");
        assert_eq!(lim.reaping.count.load(Ordering::Acquire), 1);
        assert_eq!(slot.unreaped.load(Ordering::Acquire), 1);
        // SAFETY: as above.
        assert_eq!(unsafe { libc::kill(sleeper, 0) }, 0);
        lim.reaping.ignore_kill.store(false, Ordering::Release);
        // The sleeper is reparented to init (or a subreaper), which reaps it
        // once killed; wait_released probes it with kill(pid, 0).
        wait_released(&lim, &slot, sleeper).await;
    }

    /// Same class on the blocking retirement path.
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn exited_launcher_descendants_are_killed_on_blocking_reap() {
        let dir = project("orphan-descendant-blocking");
        let pidfile = dir.join("sleeper.pid");
        let stub = dir.join("urban-forking-stub.sh");
        write_stub(
            &stub,
            &format!(
                "#!/bin/sh\nsleep 1000 </dev/null >/dev/null 2>&1 &\necho $! > '{}'\nexit 0\n",
                pidfile.display()
            ),
        );
        let lim = limiter();
        lim.reaping.short_waits.store(true, Ordering::Release);
        let mut w = spawn_worker(&stub, &dir, 0).unwrap();
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while !matches!(w.child.try_wait(), Ok(Some(_))) {
            assert!(
                std::time::Instant::now() < deadline,
                "launcher never exited"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        let sleeper: i32 = std::fs::read_to_string(&pidfile)
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        let slot = lim.workers.slot_for(&dir);
        let l = &lim;
        let s = slot.clone();
        let exited = tokio::task::block_in_place(|| kill_and_reap_blocking(l, &s, w));
        assert!(exited, "group should be killed within the grace");
        assert_eq!(lim.reaping.count.load(Ordering::Acquire), 0);
        wait_released(&lim, &slot, sleeper).await;
    }

    async fn wait_released(lim: &DataGatewayLimiter, slot: &Slot, pid: i32) {
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        // SAFETY: signal 0 only probes for existence (a reaped pid answers -1).
        while unsafe { libc::kill(pid, 0) } == 0
            || lim.reaping.count.load(Ordering::Acquire) != 0
            || slot.waiters.load(Ordering::Acquire) != 0
        {
            assert!(
                std::time::Instant::now() < deadline,
                "budget/slot not released after the worker exited"
            );
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }

    /// Deleting/renaming a project retires its worker: the process is killed
    /// and the slot forgotten, so the next op spawns fresh.
    #[tokio::test]
    async fn retire_project_worker_kills_and_forgets() {
        let dir = project("retire");
        let spawns = dir.join("spawns.txt");
        let stub = serve_stub(&dir, &spawns, 0);
        let lim = limiter();
        let a = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        drop(retire_project_worker_blocking(&lim, &dir).unwrap());
        assert!(!lim.workers.map().contains_key(&dir), "slot not forgotten");
        let pid = a["pid"].as_i64().unwrap() as i32;
        // Retirement is complete on return (no polling): the caller removes
        // or moves the directory next, which a dying worker would pin.
        // SAFETY: signal 0 only probes for existence (a reaped pid answers -1).
        assert_ne!(
            unsafe { libc::kill(pid, 0) },
            0,
            "retired worker still alive (or unreaped) when retire returned"
        );
        assert_eq!(lim.reaping.count.load(Ordering::Acquire), 0);
        let b = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        assert_ne!(a["pid"], b["pid"]);
    }

    /// #1836: deleting/renaming a project while an op is in flight kills the
    /// in-flight worker's group and waits for the op to leave before the
    /// directory is touched; while the path is tombstoned, a new op fails fast
    /// and spawns nothing; once the tombstone lifts, ops work again.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn retire_kills_in_flight_worker_and_blocks_new_ops() {
        let dir = project("retire-inflight");
        let spawns = dir.join("spawns.txt");
        let stub = gated_stub(&dir, &spawns, true);
        let lim = Arc::new(limiter());
        let (l, s, d) = (lim.clone(), stub.clone(), dir.clone());
        let op = tokio::spawn(async move {
            run_serve_op(&l, &s, &d, &serde_json::json!({"op":"write"})).await
        });
        let slot = lim.workers.slot_for(&dir);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        let pid = loop {
            if let Some(p) = slot.active().pid {
                break p;
            }
            assert!(std::time::Instant::now() < deadline, "op never started");
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        };
        let (l, d) = (lim.clone(), dir.clone());
        let (tx, rx) = tokio::sync::oneshot::channel::<()>();
        let retire = tokio::task::spawn_blocking(move || {
            let guard = retire_project_worker_blocking(&l, &d).unwrap();
            // On return the op has left the slot: no writer remains.
            assert_eq!(slot.waiters.load(Ordering::Acquire), 0);
            // SAFETY: signal 0 only probes the (killed) group for existence.
            assert_ne!(
                unsafe { libc::kill(-(pid as i32), 0) },
                0,
                "worker group alive"
            );
            let _ = rx.blocking_recv();
            drop(guard);
        });
        let r = op.await.unwrap();
        assert!(
            r.is_err(),
            "in-flight op must fail when its project is retired: {r:?}"
        );
        // Mid-delete: the tombstone refuses the op before any spawn.
        let before = spawn_count(&spawns);
        while !lim.workers.tombstones().contains_key(&dir) {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let mid = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"x"})).await;
        assert!(matches!(mid, Err(DataError::NoProject)), "{mid:?}");
        assert_eq!(spawn_count(&spawns), before, "op spawned mid-delete");
        assert!(
            !lim.workers.map().contains_key(&dir),
            "slot re-created mid-delete"
        );
        tx.send(()).unwrap();
        retire.await.unwrap();
        assert!(lim.workers.tombstones().is_empty());
        std::fs::write(dir.join("gate"), "").unwrap();
        run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"x"}))
            .await
            .unwrap();
    }

    /// #1836: a retirement landing between spawn and pid publish must reap
    /// the freshly spawned worker before the op leaves the slot, so the
    /// retire drain (waiters==0) never races a live worker in the dir.
    #[tokio::test]
    async fn retire_between_spawn_and_publish_reaps_fresh_worker() {
        let dir = project("retire-spawn");
        let spawns = dir.join("spawns.txt");
        let stub = serve_stub(&dir, &spawns, 0);
        let lim = limiter();
        let seen = Arc::new(std::sync::Mutex::new(None));
        let s2 = seen.clone();
        let slot = lim.workers.slot_for(&dir);
        *lim.after_spawn.lock().unwrap() = Some(Box::new(move |pid| {
            slot.active().retired = true;
            *s2.lock().unwrap() = pid;
        }));
        let r = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"})).await;
        assert!(matches!(r, Err(DataError::NoProject)), "{r:?}");
        let pid = seen.lock().unwrap().expect("worker spawned") as i32;
        assert_eq!(
            lim.workers.slot_for(&dir).waiters.load(Ordering::Acquire),
            0
        );
        // SAFETY: signal 0 only probes for existence (a reaped pid answers -1).
        assert_ne!(
            unsafe { libc::kill(pid, 0) },
            0,
            "fresh worker alive (or unreaped) after the retired op returned"
        );
    }

    /// #1836: a cancelled op's detached kill keeps a waiter hold on the slot
    /// until the worker is reaped, so a retire drain cannot finish early.
    #[tokio::test]
    async fn cancelled_op_holds_slot_until_reaped() {
        let dir = project("cancel-hold");
        let spawns = dir.join("spawns.txt");
        let stub = serve_stub(&dir, &spawns, 0);
        let lim = limiter();
        let slot = lim.workers.slot_for(&dir);
        let w = spawn_worker(&stub, &dir, toolkit_fingerprint(&stub, &dir)).unwrap();
        let pid = w.child.id().expect("pid") as i32;
        drop(InUse {
            limiter: &lim,
            slot: slot.clone(),
            worker: Some(w),
        });
        assert!(
            slot.waiters.load(Ordering::Acquire) > 0,
            "slot released before the cancelled worker was reaped"
        );
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while slot.waiters.load(Ordering::Acquire) != 0 {
            assert!(std::time::Instant::now() < deadline, "reap never finished");
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        // SAFETY: signal 0 only probes for existence (a reaped pid answers -1).
        assert_ne!(
            unsafe { libc::kill(pid, 0) },
            0,
            "worker alive after hold released"
        );
    }

    /// An over-long stdout line is a protocol error that kills the worker.
    #[tokio::test]
    async fn overlong_reply_line_kills_worker() {
        let dir = project("overlong");
        let stub = dir.join("urban-flood-stub.sh");
        write_stub(
            &stub,
            &format!(
                "#!/bin/sh\necho $$ > '{}'\nread -r line\nhead -c {} /dev/zero | tr '\\0' a\necho\nsleep 30\n",
                dir.join("pid").display(),
                MAX_REPLY_LINE_BYTES + 10
            ),
        );
        // Streaming 16 MiB through `head | tr` takes seconds on a loaded host;
        // the default 10s deadline would race it, so give the exchange room.
        let lim = DataGatewayLimiter::with_timeout(2, 8, std::time::Duration::from_secs(120));
        let r = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"x"})).await;
        assert!(
            matches!(r, Err(DataError::Gateway(ref m)) if m.contains("exceeds")),
            "{r:?}"
        );
        let pid: i32 = std::fs::read_to_string(dir.join("pid"))
            .unwrap()
            .trim()
            .parse()
            .unwrap();
        // SAFETY: signal 0 only probes for existence.
        assert_ne!(
            unsafe { libc::kill(pid, 0) },
            0,
            "flooding worker still alive"
        );
        assert!(lim.workers.slot_for(&dir).idle().is_none());
    }

    /// A project recreated at the same path gets a fresh worker even when the
    /// manifest and toolkit are byte-identical.
    #[tokio::test]
    async fn recreated_project_dir_restarts_worker() {
        let dir = project("recreate");
        let tk = project("recreate-tk");
        let spawns = tk.join("spawns.txt");
        let stub = serve_stub(&tk, &spawns, 0);
        let lim = limiter();
        let a = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        std::fs::remove_dir_all(&dir).unwrap();
        std::fs::create_dir_all(&dir).unwrap();
        std::fs::write(dir.join("nano.app.json"), r#"{"schemaVersion":1}"#).unwrap();
        let b = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        assert_ne!(a["pid"], b["pid"]);
        assert_eq!(spawn_count(&spawns), 2);
    }

    /// An op cancelled mid-exchange kills (and reaps) its worker by itself;
    /// the next op spawns a fresh one.
    #[tokio::test]
    async fn cancelled_op_kills_worker_without_another_request() {
        let dir = project("cancel");
        let spawns = dir.join("spawns.txt");
        let stub = dir.join("urban-slow-stub.sh");
        write_stub(
            &stub,
            &format!(
                "#!/bin/sh\necho x >> '{}'\nwhile IFS= read -r line; do\n\
                 id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9]*\\).*/\\1/p')\n\
                 case \"$line\" in *slow*) sleep 30;; esac\n\
                 printf '{{\"id\":%s,\"ok\":true,\"pid\":%s}}\\n' \"$id\" \"$$\"\ndone\n",
                spawns.display()
            ),
        );
        let lim = limiter();
        let a = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        let cancelled = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"slow"})),
        )
        .await;
        assert!(cancelled.is_err(), "slow op should have been cancelled");
        // Cancellation alone (no further request) terminates the worker.
        let pid = a["pid"].as_i64().unwrap() as i32;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        // SAFETY: signal 0 only probes for existence (a reaped pid answers -1).
        while unsafe { libc::kill(pid, 0) } == 0 && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_ne!(
            unsafe { libc::kill(pid, 0) },
            0,
            "cancelled op left its worker running"
        );
        let b = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        assert_ne!(a["pid"], b["pid"]);
        assert_eq!(spawn_count(&spawns), 2);
    }

    /// A manifest edit restarts the worker against the new declaration.
    #[tokio::test]
    async fn manifest_change_restarts_worker() {
        let dir = project("manifest");
        let spawns = dir.join("spawns.txt");
        let stub = serve_stub(&dir, &spawns, 0);
        let lim = limiter();
        let a = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        std::fs::write(
            dir.join("nano.app.json"),
            r#"{"schemaVersion":1,"datasources":[]}"#,
        )
        .unwrap();
        let b = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        assert_ne!(a["pid"], b["pid"]);
        assert_eq!(spawn_count(&spawns), 2);
    }

    /// `{ok:false,error}` replies are datasource errors, not worker failures:
    /// the worker stays warm.
    #[tokio::test]
    async fn op_error_reply_keeps_worker() {
        let dir = project("operr");
        let spawns = dir.join("spawns.txt");
        let stub = dir.join("urban-err-stub.sh");
        write_stub(
            &stub,
            &format!(
                "#!/bin/sh\necho x >> '{}'\nwhile IFS= read -r line; do\n\
                 id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9]*\\).*/\\1/p')\n\
                 printf '{{\"id\":%s,\"ok\":false,\"error\":\"bad sql\"}}\\n' \"$id\"\ndone\n",
                spawns.display()
            ),
        );
        let lim = limiter();
        for _ in 0..3 {
            let e = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"query"}))
                .await
                .unwrap_err();
            assert!(matches!(e, DataError::Op(ref m) if m == "bad sql"), "{e:?}");
        }
        assert_eq!(spawn_count(&spawns), 1);
    }

    /// Sequential ops across more projects than the concurrency never leave
    /// more than `concurrency` workers resident (#1329 bound).
    #[tokio::test]
    async fn resident_workers_are_bounded_by_concurrency() {
        let tk = project("bound-tk");
        let spawns = tk.join("spawns.txt");
        let stub = serve_stub(&tk, &spawns, 0);
        let lim = limiter(); // concurrency 2
        for i in 0..5 {
            let dir = project(&format!("bound-{i}"));
            run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
                .await
                .unwrap();
            let mut live = 0;
            for (_, slot) in lim.workers.snapshot() {
                if slot.idle().is_some() {
                    live += 1;
                }
            }
            assert!(live <= 2, "{live} resident workers exceed concurrency 2");
        }
    }

    /// A stub whose `*slow*` ops (or every op, with `all`) block until
    /// `<dir>/gate` exists — deterministic ordering, no sleeps to race.
    fn gated_stub(dir: &Path, spawns: &Path, all: bool) -> PathBuf {
        let stub = dir.join("urban-gated-stub.sh");
        let pat = if all { "*" } else { "*slow*" };
        write_stub(
            &stub,
            &format!(
                "#!/bin/sh\necho x >> '{spawns}'\nwhile IFS= read -r line; do\n\
                 id=$(printf '%s' \"$line\" | sed -n 's/.*\"id\":\\([0-9]*\\).*/\\1/p')\n\
                 case \"$line\" in {pat}) while [ ! -f '{gate}' ]; do sleep 0.05; done;; esac\n\
                 printf '{{\"id\":%s,\"ok\":true,\"pid\":%s}}\\n' \"$id\" \"$$\"\ndone\n",
                spawns = spawns.display(),
                gate = dir.join("gate").display(),
            ),
        );
        stub
    }

    /// Ops queued on one blocked project hold no execution permit, so another
    /// project still runs; and the project queue is bounded.
    #[tokio::test]
    async fn project_waiters_hold_no_permit_and_are_bounded() {
        let slow_dir = project("waiters-slow");
        let spawns = slow_dir.join("spawns.txt");
        let stub = gated_stub(&slow_dir, &spawns, false);
        let lim = Arc::new(DataGatewayLimiter::with_timeout(
            2,
            1,
            std::time::Duration::from_secs(60),
        ));
        let mut held = Vec::new();
        for _ in 0..2 {
            let (l, s, d) = (lim.clone(), stub.clone(), slow_dir.clone());
            held.push(tokio::spawn(async move {
                run_serve_op(&l, &s, &d, &serde_json::json!({"op":"slow"})).await
            }));
        }
        // Wait (bounded) until both ops are registered on the project.
        let slot = lim.workers.slot_for(&slow_dir);
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while slot.waiters.load(Ordering::Acquire) < 2 && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(slot.waiters.load(Ordering::Acquire), 2);
        // A third op for the same project exceeds holder + queue(1): Busy.
        let busy = run_serve_op(&lim, &stub, &slow_dir, &serde_json::json!({"op":"slow"})).await;
        assert!(
            matches!(busy, Err(DataError::Busy(ref m)) if m.starts_with(super::super::DATA_GATEWAY_BUSY_PREFIX)),
            "per-project Busy must carry the shared prefix (503 + Retry-After): {busy:?}"
        );
        // The queued waiter holds no permit: another project completes while
        // the blocked project cannot progress (its gate is still closed).
        let other = project("waiters-other");
        run_serve_op(&lim, &stub, &other, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        assert!(held.iter().all(|h| !h.is_finished()));
        std::fs::write(slow_dir.join("gate"), "").unwrap();
        for h in held {
            h.await.unwrap().unwrap();
        }
    }

    /// Waiting for the project's worker counts against the op's deadline: a
    /// queued op times out one timeout after it was issued, not after waiting
    /// out the holder and then a full exchange timeout of its own.
    #[tokio::test]
    async fn project_wait_shares_the_op_deadline() {
        let dir = project("deadline");
        let spawns = dir.join("spawns.txt");
        let stub = gated_stub(&dir, &spawns, true);
        let timeout = std::time::Duration::from_millis(1500);
        let lim = Arc::new(DataGatewayLimiter::with_timeout(2, 8, timeout));
        let (l, s, d) = (lim.clone(), stub.clone(), dir.clone());
        let first =
            tokio::spawn(
                async move { run_serve_op(&l, &s, &d, &serde_json::json!({"op":"a"})).await },
            );
        let slot = lim.workers.slot_for(&dir);
        while slot.waiters.load(Ordering::Acquire) < 1 {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        let t = std::time::Instant::now();
        let second = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"b"})).await;
        let elapsed = t.elapsed();
        assert!(
            matches!(second, Err(DataError::Gateway(ref m)) if m.contains("timed out")),
            "{second:?}"
        );
        // Old behaviour: ~timeout waiting + a full timeout exchanging.
        assert!(elapsed < timeout + timeout / 2, "{elapsed:?}");
        assert!(first.await.unwrap().is_err(), "gate never opens");
    }

    /// An op holding its project's turn while waiting for a permit pins no
    /// process: another op's budget sweep still evicts the project's idle
    /// worker, so resident + running never exceeds the concurrency (#1836).
    #[tokio::test]
    async fn turn_holder_awaiting_permit_does_not_pin_its_worker() {
        let dir = project("pin");
        let spawns = dir.join("spawns.txt");
        let stub = serve_stub(&dir, &spawns, 0);
        let lim = Arc::new(DataGatewayLimiter::with_timeout(
            1,
            8,
            std::time::Duration::from_secs(30),
        ));
        run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        let slot = lim.workers.slot_for(&dir);
        assert!(slot.idle().is_some(), "worker warm");
        // Another op holds the only permit; this project's next op takes its
        // turn and parks waiting for admission.
        let held = lim.acquire().await.unwrap();
        let (l, s, d) = (lim.clone(), stub.clone(), dir.clone());
        let parked = tokio::spawn(async move {
            run_serve_op(&l, &s, &d, &serde_json::json!({"op":"schema"})).await
        });
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        while slot.turn.try_lock().is_ok() && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(5)).await;
        }
        assert!(slot.turn.try_lock().is_err(), "op holds the turn");
        // The permit holder's sweep: 1 running, so 0 idle allowed.
        enforce_resident_budget(&lim);
        assert!(
            slot.idle().is_none(),
            "idle worker of a turn-holder awaiting a permit escaped eviction"
        );
        drop(held);
        parked.await.unwrap().unwrap();
    }

    /// Ops waiting for a busy project's turn consume the console-wide queue,
    /// so blocked projects cannot multiply the #1329 queue bound, and the
    /// refusal carries the Busy prefix.
    #[tokio::test]
    async fn project_turn_waiters_count_against_the_global_queue() {
        let slow_dir = project("gq-slow");
        let spawns = slow_dir.join("spawns.txt");
        let stub = gated_stub(&slow_dir, &spawns, false);
        let lim = Arc::new(DataGatewayLimiter::with_timeout(
            1,
            1,
            std::time::Duration::from_secs(60),
        ));
        let mut held = Vec::new();
        for _ in 0..2 {
            let (l, s, d) = (lim.clone(), stub.clone(), slow_dir.clone());
            held.push(tokio::spawn(async move {
                run_serve_op(&l, &s, &d, &serde_json::json!({"op":"slow"})).await
            }));
        }
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(20);
        while lim.waiting.load(Ordering::Acquire) < 1 && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
        assert_eq!(
            lim.waiting.load(Ordering::Acquire),
            1,
            "turn waiter queued once"
        );
        // The global queue (1) is full: an op for another project is refused
        // at once instead of queueing beyond the bound.
        let other = project("gq-other");
        let refused = tokio::time::timeout(
            std::time::Duration::from_secs(5),
            run_serve_op(&lim, &stub, &other, &serde_json::json!({"op":"schema"})),
        )
        .await
        .expect("refused promptly, not queued");
        assert!(
            matches!(refused, Err(DataError::Busy(ref m)) if m.starts_with(super::super::DATA_GATEWAY_BUSY_PREFIX)),
            "{refused:?}"
        );
        std::fs::write(slow_dir.join("gate"), "").unwrap();
        for h in held {
            h.await.unwrap().unwrap();
        }
        assert_eq!(
            lim.waiting.load(Ordering::Acquire),
            0,
            "queue place released"
        );
    }

    /// A teardown still in flight (an evicted or cancelled worker whose kill
    /// has not finished) holds its share of the live-process budget: an op
    /// that holds the only permit must not spawn until it has gone (#1836).
    #[tokio::test]
    async fn admission_waits_for_pending_teardown() {
        let lim = DataGatewayLimiter::with_timeout(1, 8, std::time::Duration::from_secs(10));
        let ticket = Reaping::new(&lim);
        let _permit = lim.acquire().await.unwrap();
        let waited = tokio::time::timeout(
            std::time::Duration::from_millis(100),
            lim.await_teardown_capacity(),
        )
        .await;
        assert!(waited.is_err(), "spawned over a teardown still in flight");
        let wait = lim.await_teardown_capacity();
        tokio::pin!(wait);
        drop(ticket);
        tokio::time::timeout(std::time::Duration::from_secs(5), wait)
            .await
            .expect("capacity not released once the teardown completed");
    }

    /// Retained idle workers are live processes too: with concurrency 2, two
    /// warm idle workers and one running op, evicting one leaves
    /// `running + idle + reaping = 3`, so admission must wait for the
    /// teardown rather than spawn a third (#1836).
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn admission_counts_retained_idle_workers() {
        let lim = limiter();
        lim.reaping.ignore_kill.store(true, Ordering::Release);
        lim.reaping.short_waits.store(true, Ordering::Release);
        let mut pids = Vec::new();
        for tag in ["idle-a", "idle-b"] {
            let dir = project(tag);
            let w = spawn_worker(&stubborn_stub(&dir), &dir, 0).unwrap();
            pids.push(w.child.id().unwrap());
            *lim.workers.slot_for(&dir).idle() = Some(w);
        }
        let _permit = lim.acquire().await.unwrap();
        let waited = tokio::time::timeout(
            std::time::Duration::from_millis(300),
            lim.await_teardown_capacity(),
        )
        .await;
        assert!(
            waited.is_err(),
            "spawned beside a retained idle worker and a pending teardown"
        );
        lim.reaping.ignore_kill.store(false, Ordering::Release);
        tokio::time::timeout(
            std::time::Duration::from_secs(10),
            lim.await_teardown_capacity(),
        )
        .await
        .expect("capacity not released once the teardown completed");
        for pid in pids {
            // SAFETY: best-effort cleanup of the test's own children.
            unsafe { libc::kill(pid as i32, libc::SIGKILL) };
        }
    }

    /// A group that probes alive forever after its leader is reaped (only
    /// unreaped zombies left) is released after the zombie grace, not held
    /// forever; an unreaped leader never releases (#1836).
    #[test]
    fn zombie_only_group_released_after_grace() {
        let budget = ReapBudget::default();
        budget.fake_liveness.store(1, Ordering::Release); // Signalable forever
        let t0 = Instant::now();
        let mut z = ZombieGrace::default();
        let mut now = t0;
        // Leader unreaped: never expires, however long.
        for _ in 0..1000 {
            assert!(!z.expired(&budget, Some(1), false, now));
            now += REAP_RETRY;
        }
        let reaped_at = now;
        let released = loop {
            if z.expired(&budget, Some(1), true, now) {
                break now;
            }
            now += REAP_RETRY;
        };
        assert!(released - reaped_at >= ZOMBIE_GRACE);
        assert!(released - reaped_at < ZOMBIE_GRACE + REAP_RETRY);
    }

    /// An EPERM group has a live member our SIGKILL cannot reach: it is never
    /// released as zombies, however long, even with the leader reaped (#1836).
    #[test]
    fn eperm_group_never_released() {
        let budget = ReapBudget::default();
        budget.fake_liveness.store(2, Ordering::Release);
        let mut z = ZombieGrace::default();
        let mut now = Instant::now();
        for _ in 0..1000 {
            assert!(!z.expired(&budget, Some(1), true, now));
            now += REAP_RETRY;
        }
    }

    /// End to end: a reaped worker whose group keeps probing signalable
    /// (zombies) actually releases its budget ticket and `ReapHold` after the
    /// zombie grace, while an EPERM group keeps both and retirement fails
    /// closed with `WouldBlock` (#1836).
    #[cfg(unix)]
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn zombie_group_releases_but_eperm_group_holds() {
        for (fake, name) in [(1u8, "zombie-release"), (2u8, "eperm-hold")] {
            let dir = project(name);
            let stub = stubborn_stub(&dir);
            let lim = limiter();
            lim.reaping.short_waits.store(true, Ordering::Release);
            lim.reaping.fake_liveness.store(fake, Ordering::Release);
            let w = spawn_worker(&stub, &dir, 0).unwrap();
            let pid = w.child.id().unwrap() as i32;
            let slot = lim.workers.slot_for(&dir);
            *slot.idle() = Some(w);
            let r = retire_project_worker_blocking(&lim, &dir);
            // Let the background reaper run well past the zombie grace.
            tokio::time::sleep(std::time::Duration::from_millis(1500)).await;
            // SAFETY: signal 0 only probes for existence (reaped: -1).
            assert_ne!(unsafe { libc::kill(pid, 0) }, 0, "leader not reaped");
            if fake == 1 {
                drop(r);
                assert_eq!(lim.reaping.count.load(Ordering::Acquire), 0);
                assert_eq!(slot.waiters.load(Ordering::Acquire), 0);
                assert_eq!(slot.unreaped.load(Ordering::Acquire), 0);
            } else {
                assert!(
                    matches!(&r, Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
                    "retire must fail closed over an EPERM group"
                );
                drop(r);
                assert_eq!(lim.reaping.count.load(Ordering::Acquire), 1);
                assert_eq!(slot.waiters.load(Ordering::Acquire), 1);
                let retry = retire_project_worker_blocking(&lim, &dir);
                assert!(
                    matches!(&retry, Err(e) if e.kind() == std::io::ErrorKind::WouldBlock),
                    "retry must fail closed over an EPERM group"
                );
                drop(retry);
                assert!(dir.join("nano.app.json").exists(), "directory touched");
                // Once the member is gone the hold releases.
                lim.reaping.fake_liveness.store(0, Ordering::Release);
                wait_released(&lim, &slot, pid).await;
            }
        }
    }

    /// A reaped leader whose group is empty must never be signalled by its
    /// recorded pid: the kernel may have reused it for an unrelated group
    /// (pid/pgid reuse, #1836).
    #[cfg(unix)]
    #[tokio::test]
    async fn reaped_empty_group_is_not_signalled() {
        let budget = Arc::new(ReapBudget::default());
        let mut cmd = Command::new("/bin/sh");
        cmd.args(["-c", "exit 0"]).process_group(0);
        let mut child = cmd.spawn().unwrap();
        let pid = child.id();
        child.wait().await.unwrap();
        assert!(kill_tree(&budget, &mut child, pid).await);
        budget.kill(pid, &mut child);
        assert_eq!(budget.group_signals.load(Ordering::Acquire), 0);
    }

    /// With a pending teardown the piped (embedded `data-cli.ts`) gateway refuses (Busy) instead of
    /// spawning past the bound when the teardown never finishes in time.
    #[tokio::test]
    async fn piped_gateway_respects_pending_teardown() {
        let lim = DataGatewayLimiter::with_timeout(1, 8, std::time::Duration::from_millis(150));
        let _ticket = Reaping::new(&lim);
        let mut cmd = Command::new("/bin/sh");
        cmd.args(["-c", "cat"]);
        let r = super::super::pipe_data_gateway_limited(&lim, cmd, &serde_json::json!({})).await;
        assert!(matches!(r, Err(DataError::Busy(_))), "got {r:?}");
    }

    /// An implementation update that leaves the launcher untouched (path,
    /// size, mtime) still changes the toolkit identity.
    #[test]
    fn toolkit_identity_tracks_package_implementation() {
        let pkg = project("ident");
        std::fs::create_dir_all(pkg.join("bin")).unwrap();
        std::fs::create_dir_all(pkg.join("dist")).unwrap();
        let bin = pkg.join("bin").join("urban");
        write_stub(&bin, "#!/bin/sh\n");
        std::fs::write(pkg.join("package.json"), r#"{"version":"0.100.0"}"#).unwrap();
        std::fs::write(pkg.join("dist").join("cli.js"), "a").unwrap();
        let before = crate::urban::toolkit_identity(&bin);
        let launcher = std::fs::metadata(&bin).unwrap().modified().unwrap();
        std::fs::write(pkg.join("package.json"), r#"{"version":"0.99.0"}"#).unwrap();
        std::fs::write(pkg.join("dist").join("cli.js"), "bb").unwrap();
        assert_eq!(
            std::fs::metadata(&bin).unwrap().modified().unwrap(),
            launcher
        );
        assert_ne!(before, crate::urban::toolkit_identity(&bin));
    }
}
