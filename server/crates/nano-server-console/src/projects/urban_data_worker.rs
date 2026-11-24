//! One long-lived `urban data --serve` worker per Urban project (#1836).
//!
//! The single-shot `urban data` gateway spawns a fresh Node process (pair) for
//! every Data-panel op; #1329 bounded how many run at once, but each op still
//! paid a cold Node start. `@nanobpm/urban` >= 0.100.0 ships a serve mode
//! (nanobpm/nano-ide#627): one JSON request per stdin line carrying an `id`,
//! one JSON reply per stdout line echoing it, errors as replies, a clean exit
//! on stdin EOF. This module keeps exactly one such worker alive per project
//! directory and pipes every op through it, so once the worker is warm no data
//! request starts a new process.
//!
//! Lifecycle:
//! - **Spawned lazily** on the first op for a project.
//! - **Restarted on crash**: a worker that has exited (or whose pipe broke /
//!   reached EOF mid-op) is reaped and a fresh one is spawned for the next op.
//! - **Restarted on toolkit/manifest change**: each op recomputes
//!   [`toolkit_fingerprint`] (the resolved `urban` binary + its on-disk stamp +
//!   the `nano.app.json` bytes); a mismatch retires the old worker first.
//! - **Desync-safe**: the worker stays in its slot for the whole exchange, so an
//!   op cancelled mid-exchange leaves it there marked `in_flight`; the next op
//!   kills its whole process group rather than reading a stale reply.
//! - **Retired with its project**: deleting or renaming a project calls
//!   [`retire_project_worker`], which drops the slot and kills the worker, and
//!   the fingerprint includes the directory's identity so a project recreated
//!   under the same path never reuses a worker anchored in the deleted one.
//! - **Orphan-free**: the console holds the worker's stdin, so if the console
//!   dies the worker sees EOF and exits; timeouts kill its whole process group.

use std::collections::HashMap;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, OnceLock};

use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::process::{Child, ChildStdin, ChildStdout, Command};

use super::{DataError, DataGatewayLimiter};

/// How much of the worker's most recent stderr we keep to explain a crash.
const STDERR_TAIL_BYTES: usize = 2048;
/// Grace for a retired worker to exit on stdin EOF before it is killed.
const RETIRE_GRACE: std::time::Duration = std::time::Duration::from_secs(2);

struct Worker {
    child: Child,
    stdin: ChildStdin,
    stdout: BufReader<ChildStdout>,
    fingerprint: u64,
    next_id: u64,
    /// Set while a request is written but its reply not yet read; a worker
    /// found in this state (its op was cancelled) is retired, never reused.
    in_flight: bool,
    stderr_tail: Arc<Mutex<String>>,
}

type Slot = Arc<tokio::sync::Mutex<Option<Worker>>>;

fn slots() -> std::sync::MutexGuard<'static, HashMap<PathBuf, Slot>> {
    static SLOTS: OnceLock<Mutex<HashMap<PathBuf, Slot>>> = OnceLock::new();
    SLOTS
        .get_or_init(|| Mutex::new(HashMap::new()))
        .lock()
        .unwrap_or_else(|e| e.into_inner())
}

/// The per-project worker slots, keyed on the canonical project directory.
fn slot_for(dir: &Path) -> Slot {
    slots().entry(dir.to_path_buf()).or_default().clone()
}

/// Whether `slot` is still the registered slot for `dir` (it is not once
/// [`retire_project_worker`] has dropped it).
fn slot_is_registered(dir: &Path, slot: &Slot) -> bool {
    slots().get(dir).is_some_and(|s| Arc::ptr_eq(s, slot))
}

/// Stop the project's worker (if any) and forget its slot: called when a
/// project is deleted or renamed so no idle Node worker outlives it (nor holds
/// its directory as CWD, which blocks removal on Windows). An op in flight on
/// the slot keeps its worker until it finishes, then kills it instead of
/// parking it, because the slot is no longer registered.
pub(super) fn retire_project_worker(dir: &Path) {
    // Callers key on the canonical dir; accept either spelling here.
    let canon = dunce::canonicalize(dir).ok();
    let removed: Vec<Slot> = {
        let mut map = slots();
        [Some(dir.to_path_buf()), canon]
            .into_iter()
            .flatten()
            .filter_map(|k| map.remove(&k))
            .collect()
    };
    for slot in removed {
        retire_slot(&slot);
    }
}

fn retire_slot(slot: &Slot) {
    if let Ok(mut g) = slot.try_lock()
        && let Some(mut w) = g.take()
    {
        if let Some(pid) = w.child.id() {
            super::kill_group_by_pid(pid);
        }
        // `kill_on_drop` reaps the wrapper; the group kill above got Node.
        let _ = w.child.start_kill();
    }
}

/// Identity of the toolkit + manifest a worker was started against. Any change
/// (a different resolved binary, an in-place reinstall bumping its mtime/size,
/// or any `nano.app.json` edit) yields a different value and restarts the
/// worker, so it never serves a stale toolkit or datasource declaration.
fn toolkit_fingerprint(urban: &Path, dir: &Path) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    urban.hash(&mut h);
    // `fs::metadata` follows symlinks, so a project-local `.bin/urban` link
    // reflects its (reinstalled) target.
    if let Ok(real) = std::fs::canonicalize(urban) {
        real.hash(&mut h);
    }
    if let Ok(md) = std::fs::metadata(urban) {
        md.len().hash(&mut h);
        md.modified().ok().hash(&mut h);
    }
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

/// Run one datasource op through the project's long-lived `urban data --serve`
/// worker, spawning/restarting it as needed. Admission still goes through the
/// console-wide [`DataGatewayLimiter`] (Busy back-pressure and the per-op
/// timeout, #1329); ops for one project are serialised on its worker.
pub(super) async fn run_serve_op(
    limiter: &DataGatewayLimiter,
    urban: &Path,
    dir: &Path,
    request: &serde_json::Value,
) -> Result<serde_json::Value, DataError> {
    let _permit = limiter.acquire().await?;
    let slot = slot_for(dir);
    let mut guard = slot.lock().await;
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
    // The worker stays in the slot for the whole exchange: if this op is
    // cancelled, the next op finds it `in_flight` and kills its process group.
    let result = match guard.as_mut() {
        Some(w) => {
            let id = w.next_id;
            w.next_id += 1;
            tokio::time::timeout(limiter.timeout, exchange(w, id, request)).await
        }
        None => Ok(Err(DataError::Gateway(
            "internal error: data worker slot empty".to_string(),
        ))),
    };
    let ok = matches!(result, Ok(Ok(_)));
    if !ok || !slot_is_registered(dir, &slot) {
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
        Err(_) => Err(DataError::Gateway(format!(
            "data gateway timed out after {:?}",
            limiter.timeout
        ))),
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
            && !stderr_seen(&slot_for(&dir)).await
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
        let mut g = slot.lock().await;
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
        retire_project_worker(&dir);
        assert!(!slots().contains_key(&dir), "slot not forgotten");
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

    /// An op cancelled mid-exchange leaves the worker in its slot marked
    /// `in_flight`; the next op kills it and spawns a fresh one.
    #[tokio::test]
    async fn cancelled_op_retires_worker_on_next_op() {
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
        {
            let slot = slot_for(&dir);
            let g = slot.lock().await;
            assert!(
                g.as_ref().is_some_and(|w| w.in_flight),
                "worker not kept in_flight"
            );
        }
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
}
