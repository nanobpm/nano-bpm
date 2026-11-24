//! One long-lived `urban data --serve` worker per Urban project (#1836).
//!
//! The single-shot `urban data` gateway spawns a fresh Node process (pair) for
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
//!   admitted op (serve *or* single-shot) evicts least-recently-used idle
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
    /// Ops waiting for (or holding) this project's turn.
    waiters: AtomicUsize,
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
}

impl WorkerRegistry {
    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<PathBuf, Slot>> {
        self.slots.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn tombstones(&self) -> std::sync::MutexGuard<'_, HashMap<PathBuf, usize>> {
        self.tombstones.lock().unwrap_or_else(|e| e.into_inner())
    }

    #[cfg(test)]
    fn slot_for(&self, dir: &Path) -> Slot {
        self.map().entry(dir.to_path_buf()).or_default().clone()
    }

    /// The slot for `dir`, unless its directory is being deleted/renamed.
    fn live_slot_for(&self, dir: &Path) -> Option<Slot> {
        let mut map = self.map();
        if self.tombstones().contains_key(dir) {
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
    }
}

fn kill_and_reap_blocking(limiter: &DataGatewayLimiter, mut w: Worker) {
    let _ticket = Reaping::new(limiter);
    if let Some(pid) = w.child.id() {
        super::kill_group_by_pid(pid);
    }
    let _ = w.child.start_kill();
    let give_up = Instant::now() + super::GATEWAY_REAP_GRACE;
    while !matches!(w.child.try_wait(), Ok(Some(_)) | Err(_)) && Instant::now() < give_up {
        std::thread::sleep(Duration::from_millis(10));
    }
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
        for k in &keys {
            *t.entry(k.clone()).or_default() += 1;
        }
        keys.iter().filter_map(|k| map.remove(k)).collect()
    };
    let guard = Retired {
        registry: &limiter.workers,
        keys,
    };
    for slot in &removed {
        let pid = {
            let mut a = slot.active();
            a.retired = true;
            a.pid
        };
        if let Some(pid) = pid {
            // The in-flight op sees EOF, fails, and leaves the slot.
            super::kill_group_by_pid(pid);
        }
        let idle = slot.idle().take();
        if let Some(w) = idle {
            kill_and_reap_blocking(limiter, w);
        }
    }
    let give_up = Instant::now() + RETIRE_DRAIN;
    while removed
        .iter()
        .any(|s| s.waiters.load(Ordering::Acquire) > 0)
    {
        if Instant::now() >= give_up {
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
/// [`DataGatewayLimiter`]'s `reaping` budget, since it is still a live process.
fn kill_detached(limiter: &DataGatewayLimiter, mut w: Worker) {
    let pid = w.child.id();
    let ticket = Reaping::new(limiter);
    // The Unix group signal is a cheap syscall: send it now so the worker
    // stops at once, whatever happens to the background task.
    #[cfg(unix)]
    if let Some(pid) = pid {
        super::kill_group_by_pid(pid);
    }
    let Ok(rt) = tokio::runtime::Handle::try_current() else {
        // No runtime (shutdown): best effort, inline. `kill_on_drop` backs it.
        #[cfg(not(unix))]
        if let Some(pid) = pid {
            super::kill_group_by_pid(pid);
        }
        let _ = w.child.start_kill();
        drop(ticket);
        return;
    };
    rt.spawn(async move {
        let _ticket = ticket;
        #[cfg(not(unix))]
        if let Some(pid) = pid {
            let _ = tokio::task::spawn_blocking(move || super::kill_group_by_pid(pid)).await;
        }
        let _ = w.child.start_kill();
        super::reap_gateway_bounded(&mut w.child, super::GATEWAY_REAP_GRACE).await;
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
            let w = slot.idle().take();
            if let Some(w) = w {
                tracing::debug!(pid = ?w.child.id(), "data worker evicted (resident budget)");
                kill_detached(limiter, w);
            }
        }
    }
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

/// Kill a child's whole process tree and reap it (bounded).
async fn kill_tree(child: &mut Child) {
    if let Some(pid) = child.id() {
        // Blocking on Windows (`taskkill` status); cheap signal on Unix.
        let _ = tokio::task::spawn_blocking(move || super::kill_group_by_pid(pid)).await;
    }
    super::reap_gateway_bounded(child, super::GATEWAY_REAP_GRACE).await;
}

async fn kill_worker(mut w: Worker) {
    kill_tree(&mut w.child).await;
}

/// Retire a healthy worker gracefully: close stdin (serve mode exits 0 on EOF)
/// and kill it only if it doesn't go within [`RETIRE_GRACE`].
async fn retire_worker(w: Worker) {
    let Worker {
        mut child, stdin, ..
    } = w;
    drop(stdin);
    if tokio::time::timeout(RETIRE_GRACE, child.wait())
        .await
        .is_err()
    {
        kill_tree(&mut child).await;
    }
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
            kill_detached(self.limiter, w);
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
    dir: &Path,
    request: &serde_json::Value,
) -> Result<serde_json::Value, DataError> {
    let deadline = tokio::time::Instant::now() + limiter.timeout;
    let retired = || DataError::NoProject;
    let slot = limiter.workers.live_slot_for(dir).ok_or_else(retired)?;
    // Bound the per-project queue: at most one holder + `queue` waiters.
    let prev = slot.waiters.fetch_add(1, Ordering::AcqRel);
    let _waiting = Waiting(slot.clone());
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
    // Take our worker first: from here it is counted by our permit.
    if slot.active().retired {
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
            kill_worker(w).await;
        } else if w.fingerprint != fingerprint {
            tracing::debug!(dir = %dir.display(), "data worker restart: toolkit/manifest changed");
            retire_worker(w).await;
        } else {
            use_.worker = Some(w);
        }
    }
    if use_.worker.is_none() {
        use_.worker = Some(spawn_worker(urban, dir, fingerprint)?);
    }
    {
        // Publish the in-use pid so retirement can kill it; if the project
        // was retired meanwhile, this worker must not run (InUse kills it).
        let mut a = slot.active();
        if a.retired {
            drop(a);
            return Err(retired());
        }
        a.pid = use_.worker.as_ref().and_then(|w| w.child.id());
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
            kill_worker(w).await;
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
        let lim = limiter();
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

    /// With a pending teardown the single-shot path refuses (Busy) instead of
    /// spawning past the bound when the teardown never finishes in time.
    #[tokio::test]
    async fn single_shot_respects_pending_teardown() {
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
