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
//!   [`retire_project_worker`], which drops the slot and kills the worker.
//! - **Orphan-free**: the console holds the worker's stdin, so if the console
//!   dies the worker sees EOF and exits; timeouts kill its whole process group.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use super::{DataError, DataGatewayLimiter};

/// How much of the worker's most recent stderr we keep to explain a crash.
const STDERR_TAIL_BYTES: usize = 2048;
/// Grace for a retired worker to exit on stdin EOF before it is killed.
const RETIRE_GRACE: Duration = Duration::from_secs(2);
/// An idle worker unused for this long is retired on the next sweep.
const IDLE_TTL: Duration = Duration::from_secs(300);

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
    /// Ops waiting for (or holding) this project's worker.
    waiters: AtomicUsize,
    worker: tokio::sync::Mutex<Option<Worker>>,
}

type Slot = Arc<SlotInner>;

/// The per-project worker slots, keyed on the canonical project directory.
/// Owned by a [`DataGatewayLimiter`], so its budget bounds them.
#[derive(Default)]
pub(crate) struct WorkerRegistry(Mutex<HashMap<PathBuf, Slot>>);

impl WorkerRegistry {
    fn map(&self) -> std::sync::MutexGuard<'_, HashMap<PathBuf, Slot>> {
        self.0.lock().unwrap_or_else(|e| e.into_inner())
    }

    fn slot_for(&self, dir: &Path) -> Slot {
        self.map().entry(dir.to_path_buf()).or_default().clone()
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

/// Stop the project's worker (if any) and forget its slot: called when a
/// project is deleted or renamed so no idle Node worker outlives it (nor holds
/// its directory as CWD, which blocks removal on Windows). An op in flight on
/// the slot keeps its worker until it finishes, then kills it instead of
/// parking it, because the slot is no longer registered.
pub(super) fn retire_project_worker(limiter: &DataGatewayLimiter, dir: &Path) {
    // Callers key on the canonical dir; accept either spelling here.
    let canon = dunce::canonicalize(dir).ok();
    let removed: Vec<Slot> = {
        let mut map = limiter.workers.map();
        [Some(dir.to_path_buf()), canon]
            .into_iter()
            .flatten()
            .filter_map(|k| map.remove(&k))
            .collect()
    };
    for slot in removed {
        if let Ok(mut g) = slot.worker.try_lock()
            && let Some(w) = g.take()
        {
            kill_now(w);
        }
    }
}

/// Kill a worker's process group synchronously-ish (no await) and leave the
/// wrapper to `kill_on_drop`.
fn kill_now(mut w: Worker) {
    if let Some(pid) = w.child.id() {
        super::kill_group_by_pid(pid);
    }
    let _ = w.child.start_kill();
}

/// Enforce the console-wide live-process budget (#1329). Called by every op
/// right after it is admitted (holding a permit): idle workers of *other*
/// projects are retired, least-recently-used first, until
/// `idle resident + running ops <= concurrency`; any idle past [`IDLE_TTL`]
/// goes too. Busy slots (an op holds their lock) are running ops already
/// counted by the permits.
pub(super) fn enforce_resident_budget(limiter: &DataGatewayLimiter, keep: Option<&Path>) {
    let running = limiter
        .concurrency
        .saturating_sub(limiter.permits.available_permits());
    let allowed_idle = limiter.concurrency.saturating_sub(running);
    let mut guards = Vec::new();
    for (dir, slot) in limiter.workers.snapshot() {
        if keep == Some(dir.as_path()) {
            continue;
        }
        let last = match slot.worker.try_lock() {
            Ok(g) => g.as_ref().map(|w| w.last_used),
            Err(_) => None,
        };
        if let Some(last) = last {
            guards.push((last, slot));
        }
    }
    // Newest first; retire everything past the allowance or past the TTL.
    guards.sort_by_key(|g| std::cmp::Reverse(g.0));
    for (i, (last, slot)) in guards.into_iter().enumerate() {
        if (i >= allowed_idle || last.elapsed() >= IDLE_TTL)
            && let Ok(mut g) = slot.worker.try_lock()
            && let Some(w) = g.take()
        {
            tracing::debug!(pid = ?w.child.id(), "data worker evicted (resident budget)");
            kill_now(w);
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
        let mut lines = BufReader::new(stderr).lines();
        while let Ok(Some(line)) = lines.next_line().await {
            tracing::debug!(target: "data_worker", "{line}");
            let mut t = tail.lock().unwrap_or_else(|e| e.into_inner());
            t.push_str(&line);
            t.push('\n');
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
        let mut buf = String::new();
        let n = match w.stdout.read_line(&mut buf).await {
            Ok(n) => n,
            Err(e) => return Err(with_stderr(w, format!("data worker read failed: {e}"))),
        };
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
            // it could not parse (only ever ours: one request is in flight).
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

/// Armed while a request is on the wire: if the op's future is dropped
/// (caller cancelled) before the reply is read, kill the worker's process
/// group immediately and reap it in the background — no later request is
/// needed to stop a hung or abandoned operation.
struct InFlightGuard {
    pid: Option<u32>,
    slot: Slot,
    armed: bool,
}

impl Drop for InFlightGuard {
    fn drop(&mut self) {
        if !self.armed {
            return;
        }
        if let Some(pid) = self.pid {
            super::kill_group_by_pid(pid);
        }
        let slot = self.slot.clone();
        let pid = self.pid;
        if let Ok(rt) = tokio::runtime::Handle::try_current() {
            rt.spawn(async move {
                let mut g = slot.worker.lock().await;
                if g.as_ref()
                    .is_some_and(|w| w.in_flight && w.child.id() == pid)
                    && let Some(w) = g.take()
                {
                    kill_worker(w).await;
                }
            });
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
    let slot = limiter.workers.slot_for(dir);
    // Bound the per-project queue: at most one holder + `queue` waiters.
    let prev = slot.waiters.fetch_add(1, Ordering::AcqRel);
    let _waiting = Waiting(slot.clone());
    if prev > limiter.queue {
        return Err(DataError::Busy(format!(
            "{} data ops already queued for this project; retry shortly",
            limiter.queue
        )));
    }
    let mut guard = tokio::time::timeout_at(deadline, slot.worker.lock())
        .await
        .map_err(|_| timed_out(limiter))?;
    let _permit = tokio::time::timeout_at(deadline, limiter.acquire())
        .await
        .map_err(|_| timed_out(limiter))??;
    enforce_resident_budget(limiter, Some(dir));
    let fingerprint = toolkit_fingerprint(urban, dir);

    if let Some(mut w) = guard.take() {
        let alive = matches!(w.child.try_wait(), Ok(None));
        if !alive || w.in_flight {
            tracing::debug!(dir = %dir.display(), alive, in_flight = w.in_flight, "data worker restart");
            kill_worker(w).await;
        } else if w.fingerprint != fingerprint {
            tracing::debug!(dir = %dir.display(), "data worker restart: toolkit/manifest changed");
            retire_worker(w).await;
        } else {
            *guard = Some(w);
        }
    }
    if guard.is_none() {
        *guard = Some(spawn_worker(urban, dir, fingerprint)?);
    }
    let result = match guard.as_mut() {
        Some(w) => {
            let id = w.next_id;
            w.next_id += 1;
            w.last_used = Instant::now();
            let mut armed = InFlightGuard {
                pid: w.child.id(),
                slot: slot.clone(),
                armed: true,
            };
            let r = tokio::time::timeout_at(deadline, exchange(w, id, request)).await;
            armed.armed = false;
            w.last_used = Instant::now();
            r
        }
        None => Ok(Err(DataError::Gateway(
            "internal error: data worker slot empty".to_string(),
        ))),
    };
    let ok = matches!(result, Ok(Ok(_)));
    if !ok || !limiter.workers.is_registered(dir, &slot) {
        // Failed/timed-out (possibly desynced) worker, or one whose project was
        // retired while this op ran: never park it.
        if let Some(w) = guard.take() {
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
        let mut g = slot.worker.lock().await;
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
        retire_project_worker(&lim, &dir);
        assert!(!lim.workers.map().contains_key(&dir), "slot not forgotten");
        let pid = a["pid"].as_i64().unwrap() as i32;
        let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
        // SAFETY: signal 0 only probes for existence (a reaped pid answers -1).
        while unsafe { libc::kill(pid, 0) } == 0 && std::time::Instant::now() < deadline {
            tokio::time::sleep(std::time::Duration::from_millis(20)).await;
        }
        assert_ne!(
            unsafe { libc::kill(pid, 0) },
            0,
            "retired worker still alive"
        );
        let b = run_serve_op(&lim, &stub, &dir, &serde_json::json!({"op":"schema"}))
            .await
            .unwrap();
        assert_ne!(a["pid"], b["pid"]);
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
                if slot.worker.lock().await.is_some() {
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
        assert!(matches!(busy, Err(DataError::Busy(_))), "{busy:?}");
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
