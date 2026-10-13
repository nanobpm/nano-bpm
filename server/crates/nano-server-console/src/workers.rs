//! Worker supervisor for the console.
//!
//! Runs each enabled worker as a **Node** subprocess (one process per worker),
//! independent of any browser. The worker code (`workers/<name>/worker.ts`)
//! imports the embedded SDK (`nano-generated/worker-sdk.ts`), which speaks the
//! falcon protocol and prints structured metric/status lines that this
//! supervisor parses. Everything here is feature-gated behind `console`; the
//! base gateway build never spawns Node and does not require it on PATH.
//!
//! Node (>= 22.6) is the sole JS/TS runtime (ADR 0065 removed Deno). If it is
//! too old or absent, the Workers tab still authors code but `start` reports the
//! runtime as unavailable.

use std::collections::{HashMap, VecDeque};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, AtomicU16, AtomicU32, Ordering};
use std::sync::{Arc, OnceLock};
use std::time::{SystemTime, UNIX_EPOCH};

use serde::Serialize;
use tokio::io::{AsyncBufReadExt, BufReader};
use tokio::process::Command;
use tokio::sync::{Mutex, Notify, broadcast};

use super::workspace;

/// The worker SDK source, baked into the binary and materialised on disk so
/// worker code can import it. Kept in sync with the binary on every supervisor
/// init (overwritten), so upgrading the server upgrades the SDK.
const WORKER_SDK_TS: &str = include_str!("worker_sdk.ts");
/// Node runtime loader + bootstrap, materialised beside the SDK so the
/// project's import-map manifest resolves under Node.
const NODE_LOADER_MJS: &str = include_str!("node_loader.mjs");
const NODE_REGISTER_MJS: &str = include_str!("node_register.mjs");

/// Control-line prefixes emitted by the SDK on stdout (see `worker_sdk.ts`).
const METRIC_PREFIX: &str = "@@NBPM_METRIC@@";
const STATUS_PREFIX: &str = "@@NBPM_STATUS@@";

/// Max log lines retained per worker for replay to a freshly-opened log view.
const LOG_RING_CAP: usize = 500;

fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as u64)
        .unwrap_or(0)
}

// ---------------------------------------------------------------------------
// Runtime model
// ---------------------------------------------------------------------------

#[derive(Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
enum Phase {
    Stopped,
    Starting,
    Running,
    Crashed,
}

#[derive(Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct LogLine {
    pub ts_ms: u64,
    /// `out` (stdout), `err` (stderr), or `sys` (supervisor/status notes).
    pub stream: String,
    pub text: String,
}

#[derive(Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
struct Metrics {
    completed: u64,
    failed: u64,
    in_flight: i64,
    throughput: f64,
    uptime_ms: u64,
    connected: bool,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
pub struct WorkerRuntimeDto {
    status: Phase,
    pid: Option<u32>,
    started_at_ms: Option<u64>,
    restarts: u32,
    last_error: Option<String>,
    metrics: Metrics,
}

/// Per-worker shared runtime. Each field has its own small lock so the line
/// readers never contend on a process-global map.
struct WorkerInner {
    phase: Mutex<Phase>,
    metrics: Mutex<Metrics>,
    last_error: Mutex<Option<String>>,
    started_at_ms: Mutex<Option<u64>>,
    pid: AtomicU32,
    restarts: AtomicU32,
    desired_running: AtomicBool,
    /// Signals the supervising task to terminate the subprocess.
    stop: Notify,
    logs_tx: broadcast::Sender<LogLine>,
    log_ring: Mutex<VecDeque<LogLine>>,
}

impl WorkerInner {
    fn new() -> Arc<Self> {
        let (logs_tx, _) = broadcast::channel(256);
        Arc::new(Self {
            phase: Mutex::new(Phase::Stopped),
            metrics: Mutex::new(Metrics::default()),
            last_error: Mutex::new(None),
            started_at_ms: Mutex::new(None),
            pid: AtomicU32::new(0),
            restarts: AtomicU32::new(0),
            desired_running: AtomicBool::new(false),
            stop: Notify::new(),
            logs_tx,
            log_ring: Mutex::new(VecDeque::with_capacity(LOG_RING_CAP)),
        })
    }

    async fn push_log(&self, stream: &str, text: String) {
        let line = LogLine {
            ts_ms: now_ms(),
            stream: stream.to_string(),
            text,
        };
        {
            let mut ring = self.log_ring.lock().await;
            if ring.len() >= LOG_RING_CAP {
                ring.pop_front();
            }
            ring.push_back(line.clone());
        }
        let _ = self.logs_tx.send(line);
    }

    async fn dto(&self) -> WorkerRuntimeDto {
        let pid = self.pid.load(Ordering::Relaxed);
        WorkerRuntimeDto {
            status: *self.phase.lock().await,
            pid: (pid != 0).then_some(pid),
            started_at_ms: *self.started_at_ms.lock().await,
            restarts: self.restarts.load(Ordering::Relaxed),
            last_error: self.last_error.lock().await.clone(),
            metrics: self.metrics.lock().await.clone(),
        }
    }
}

// ---------------------------------------------------------------------------
// Supervisor
// ---------------------------------------------------------------------------

pub struct WorkerSupervisor {
    gateway_port: AtomicU16,
    workers: Mutex<HashMap<String, Arc<WorkerInner>>>,
}

static SUPERVISOR: OnceLock<WorkerSupervisor> = OnceLock::new();

/// The process-global worker supervisor. Materialises the embedded SDK to disk
/// on first access (best-effort).
pub fn supervisor() -> &'static WorkerSupervisor {
    SUPERVISOR.get_or_init(|| {
        let _ = ensure_sdk_written();
        WorkerSupervisor {
            gateway_port: AtomicU16::new(8080),
            workers: Mutex::new(HashMap::new()),
        }
    })
}

/// Records the gateway's actually-bound port so spawned workers can dial the
/// Falcon protocol on `127.0.0.1:<port>`. Called from `main` after bind.
pub fn set_gateway_port(port: u16) {
    supervisor().gateway_port.store(port, Ordering::Relaxed);
}

/// The gateway's bound port (for composing a default deploy/base URL).
pub(crate) fn gateway_port() -> u16 {
    supervisor().gateway_port.load(Ordering::Relaxed)
}

/// Writes the embedded worker SDK to `<workspace>/nano-generated/worker-sdk.ts`,
/// overwriting any prior copy so it tracks the running binary.
fn ensure_sdk_written() -> std::io::Result<()> {
    let dir = workspace::sdk_dir();
    std::fs::create_dir_all(&dir)?;
    std::fs::write(dir.join("node-loader.mjs"), NODE_LOADER_MJS)?;
    std::fs::write(dir.join("node-register.mjs"), NODE_REGISTER_MJS)?;
    std::fs::write(workspace::sdk_path(), WORKER_SDK_TS)
}

/// Backfills the `@lib/` import-map alias into a worker's `node.json` so workers
/// scaffolded before shared-library support can still `import "@lib/…"`. Only
/// adds the missing alias; never disturbs other user-defined imports. Best
/// effort — a malformed or missing `node.json` is left untouched.
fn ensure_lib_alias(dir: &std::path::Path) {
    let path = dir.join("node.json");
    let Ok(text) = std::fs::read_to_string(&path) else {
        return;
    };
    let Ok(mut json) = serde_json::from_str::<serde_json::Value>(&text) else {
        return;
    };
    let imports = json.as_object_mut().and_then(|o| {
        o.entry("imports")
            .or_insert_with(|| serde_json::json!({}))
            .as_object_mut()
    });
    let Some(imports) = imports else { return };
    if imports.contains_key("@lib/") {
        return;
    }
    imports.insert("@lib/".to_string(), serde_json::json!("../../lib/"));
    if let Ok(pretty) = serde_json::to_string_pretty(&json) {
        let _ = std::fs::write(&path, format!("{pretty}\n"));
    }
}

/// Minimum Node that can run workers: `--experimental-strip-types` (TS erasure)
/// landed in 22.6; `node:sqlite` (22.5) and a global `WebSocket` (22.4) are
/// older still, so 22.6 is the effective floor.
const NODE_MIN: (u32, u32) = (22, 6);

/// Locates a Node binary for the worker fallback runtime: `NANOBPMN_NODE_BIN`
/// (exported by the npm launcher — its own `process.execPath`), then `PATH`,
/// then `~/.local/bin`. Presence only; see [`usable_node`] for the version gate.
pub(crate) fn find_node() -> Option<PathBuf> {
    if let Ok(p) = std::env::var("NANOBPMN_NODE_BIN")
        && !p.is_empty()
    {
        let pb = PathBuf::from(p);
        if pb.is_file() {
            return Some(pb);
        }
    }
    let exe = if cfg!(windows) { "node.exe" } else { "node" };
    if let Ok(path) = std::env::var("PATH") {
        for dir in std::env::split_paths(&path) {
            let cand = dir.join(exe);
            if cand.is_file() {
                return Some(cand);
            }
        }
    }
    if let Some(home) = std::env::var_os("HOME") {
        let cand = PathBuf::from(home).join(".local").join("bin").join(exe);
        if cand.is_file() {
            return Some(cand);
        }
    }
    None
}

/// Parses a `node --version` string (`"v24.15.0\n"`) into `(major, minor)`.
fn parse_node_version(s: &str) -> Option<(u32, u32)> {
    let v = s.trim().trim_start_matches('v');
    let mut parts = v.split('.');
    let major = parts.next()?.parse().ok()?;
    let minor = parts.next().unwrap_or("0").parse().ok()?;
    Some((major, minor))
}

/// The Node binary iff present **and** new enough ([`NODE_MIN`]) to run workers.
/// Probes `node --version`; returns `None` on any older/unparseable/erroring Node.
pub(crate) fn usable_node() -> Option<PathBuf> {
    let bin = find_node()?;
    let out = std::process::Command::new(&bin)
        .arg("--version")
        .output()
        .ok()?;
    if !out.status.success() {
        return None;
    }
    let ver = parse_node_version(&String::from_utf8_lossy(&out.stdout))?;
    (ver >= NODE_MIN).then_some(bin)
}

impl WorkerSupervisor {
    /// Whether a usable Node (>= 22.6) runtime is available — the sole runtime
    /// for running workers (ADR 0065 removed Deno).
    pub fn node_available(&self) -> bool {
        usable_node().is_some()
    }

    async fn entry(&self, name: &str) -> Arc<WorkerInner> {
        let mut map = self.workers.lock().await;
        map.entry(name.to_string())
            .or_insert_with(WorkerInner::new)
            .clone()
    }

    /// Runtime view of one worker (defaults to a stopped runtime if unknown).
    pub async fn runtime(&self, name: &str) -> WorkerRuntimeDto {
        self.entry(name).await.dto().await
    }

    /// Replay buffer of a worker's recent log lines.
    pub async fn log_history(&self, name: &str) -> Vec<LogLine> {
        let inner = self.entry(name).await;
        let ring = inner.log_ring.lock().await;
        ring.iter().cloned().collect()
    }

    /// Subscribe to a worker's live log stream.
    pub async fn subscribe(&self, name: &str) -> broadcast::Receiver<LogLine> {
        self.entry(name).await.logs_tx.subscribe()
    }

    /// Starts (or restarts) a worker subprocess. Idempotent if already running.
    pub async fn start(&self, name: &str) -> Result<(), String> {
        let Some(dir) = workspace::worker_dir(name) else {
            return Err("invalid worker name".into());
        };
        if !dir.is_dir() {
            return Err("no such worker".into());
        }
        // Node (>= 22.6) is the sole runtime — the npm launcher always provides
        // one (ADR 0065 removed Deno).
        let Some(node) = usable_node() else {
            return Err("No JavaScript runtime found to run workers. Install Node \
                 >= 22.6 (the npm launcher provides one)."
                .to_string());
        };

        let inner = self.entry(name).await;
        if matches!(*inner.phase.lock().await, Phase::Starting | Phase::Running) {
            return Ok(()); // already running
        }
        // A start that follows a prior run (stopped or crashed) is a restart.
        if inner.started_at_ms.lock().await.is_some() {
            inner.restarts.fetch_add(1, Ordering::Relaxed);
        }
        // Refresh the on-disk SDK so worker code imports the current version.
        let _ = ensure_sdk_written();
        // Backfill the @lib/ alias for workers created before shared-lib support.
        ensure_lib_alias(&dir);
        // The shared library dir must exist for `../../lib/` to resolve.
        let _ = workspace::ensure_lib_dir();

        let entrypoint = dir.join("worker.ts");
        if !entrypoint.is_file() {
            return Err("worker has no worker.ts entrypoint".into());
        }
        let port = self.gateway_port.load(Ordering::Relaxed);
        let base_url = format!("http://127.0.0.1:{port}");

        // Strip TS types + register the import-map loader (materialised beside the
        // SDK). `node:sqlite`/WebSocket are built in.
        let mut cmd = Command::new(node);
        cmd.current_dir(&dir);
        super::projects::node_fallback_command(
            &mut cmd,
            &workspace::sdk_dir().join("node-register.mjs"),
        );
        cmd.arg("worker.ts");
        cmd.env("NO_COLOR", "1")
            .env("NANOBPMN_BASE_URL", &base_url)
            .env("NANOBPMN_WORKER_NAME", name)
            .stdin(std::process::Stdio::null())
            .stdout(std::process::Stdio::piped())
            .stderr(std::process::Stdio::piped())
            .kill_on_drop(true);

        let mut child = cmd
            .spawn()
            .map_err(|e| format!("failed to spawn node: {e}"))?;

        let pid = child.id().unwrap_or(0);
        inner.pid.store(pid, Ordering::Relaxed);
        inner.desired_running.store(true, Ordering::Relaxed);
        *inner.phase.lock().await = Phase::Starting;
        *inner.started_at_ms.lock().await = Some(now_ms());
        *inner.last_error.lock().await = None;
        *inner.metrics.lock().await = Metrics::default();
        inner
            .push_log("sys", format!("starting worker (pid {pid})"))
            .await;

        // stdout reader: parse metric/status control lines, log the rest.
        if let Some(stdout) = child.stdout.take() {
            let inner = inner.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stdout).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    if let Some(rest) = line.strip_prefix(METRIC_PREFIX) {
                        if let Ok(m) = serde_json::from_str::<MetricLine>(rest) {
                            let mut metrics = inner.metrics.lock().await;
                            metrics.completed = m.completed;
                            metrics.failed = m.failed;
                            metrics.in_flight = m.in_flight;
                            metrics.throughput = m.throughput;
                            metrics.uptime_ms = m.uptime_ms;
                            metrics.connected = m.connected;
                            drop(metrics);
                            if let Some(e) = m.last_error {
                                *inner.last_error.lock().await = Some(e);
                            }
                        }
                    } else if let Some(rest) = line.strip_prefix(STATUS_PREFIX) {
                        if let Ok(s) = serde_json::from_str::<StatusLine>(rest) {
                            if s.state == "running" {
                                *inner.phase.lock().await = Phase::Running;
                            } else if s.state == "error" {
                                *inner.last_error.lock().await = Some(s.message.clone());
                            }
                            inner
                                .push_log("sys", format!("{}: {}", s.state, s.message))
                                .await;
                        }
                    } else {
                        inner.push_log("out", line).await;
                    }
                }
            });
        }
        // stderr reader: log lines. Node writes informational warnings to
        // stderr too, so these are surfaced as logs but NOT recorded as
        // `last_error`; genuine errors come from the SDK's status/metric lines
        // and the process exit code.
        if let Some(stderr) = child.stderr.take() {
            let inner = inner.clone();
            tokio::spawn(async move {
                let mut lines = BufReader::new(stderr).lines();
                while let Ok(Some(line)) = lines.next_line().await {
                    inner.push_log("err", line).await;
                }
            });
        }

        // Supervising task: wait for either an explicit stop or process exit.
        let inner = inner.clone();
        tokio::spawn(async move {
            let status = tokio::select! {
                _ = inner.stop.notified() => {
                    let _ = child.start_kill();
                    inner.push_log("sys", "stop requested; terminating".into()).await;
                    child.wait().await.ok()
                }
                st = child.wait() => st.ok(),
            };
            inner.pid.store(0, Ordering::Relaxed);
            inner.metrics.lock().await.connected = false;
            let desired = inner.desired_running.load(Ordering::Relaxed);
            let code = status.and_then(|s| s.code());
            if desired {
                // Exited without a stop request: a crash.
                *inner.phase.lock().await = Phase::Crashed;
                inner.desired_running.store(false, Ordering::Relaxed);
                inner
                    .push_log("sys", format!("worker exited unexpectedly (code {code:?})"))
                    .await;
            } else {
                *inner.phase.lock().await = Phase::Stopped;
                inner.push_log("sys", "worker stopped".into()).await;
            }
        });

        Ok(())
    }

    /// Stops a running worker. No-op if it is not running.
    pub async fn stop(&self, name: &str) -> Result<(), String> {
        let inner = self.entry(name).await;
        if matches!(*inner.phase.lock().await, Phase::Stopped | Phase::Crashed) {
            return Ok(());
        }
        inner.desired_running.store(false, Ordering::Relaxed);
        inner.stop.notify_waiters();
        Ok(())
    }

    /// Whether a worker is currently starting or running (blocks delete).
    pub async fn is_active(&self, name: &str) -> bool {
        let inner = self.entry(name).await;
        matches!(*inner.phase.lock().await, Phase::Starting | Phase::Running)
    }
}

#[derive(serde::Deserialize)]
#[serde(rename_all = "camelCase")]
struct MetricLine {
    #[serde(default)]
    completed: u64,
    #[serde(default)]
    failed: u64,
    #[serde(default)]
    in_flight: i64,
    #[serde(default)]
    throughput: f64,
    #[serde(default)]
    uptime_ms: u64,
    #[serde(default)]
    connected: bool,
    #[serde(default)]
    last_error: Option<String>,
}

#[derive(serde::Deserialize)]
struct StatusLine {
    state: String,
    #[serde(default)]
    message: String,
}

#[cfg(test)]
mod node_runtime_tests {
    use super::{NODE_MIN, find_node, parse_node_version};

    #[test]
    fn parse_node_version_reads_major_minor() {
        assert_eq!(parse_node_version("v24.15.0\n"), Some((24, 15)));
        assert_eq!(parse_node_version("v22.6.1"), Some((22, 6)));
        assert_eq!(parse_node_version("v22"), Some((22, 0)));
    }

    #[test]
    fn parse_node_version_rejects_garbage() {
        assert_eq!(parse_node_version(""), None);
        assert_eq!(parse_node_version("not-a-version"), None);
        assert_eq!(parse_node_version("vx.y.z"), None);
    }

    #[test]
    fn node_min_floor_gates_type_stripping() {
        // Type stripping + node:sqlite require >= 22.6; keep the floor there.
        assert_eq!(NODE_MIN, (22, 6));
        assert!(parse_node_version("v22.5.0").unwrap() < NODE_MIN);
        assert!(parse_node_version("v22.6.0").unwrap() >= NODE_MIN);
    }

    #[test]
    fn find_node_honors_explicit_bin_env() {
        // A non-existent path is ignored; a real file is returned.
        let dir = std::env::temp_dir().join(format!("nbpm-node-{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let fake = dir.join(if cfg!(windows) { "node.exe" } else { "node" });
        std::fs::write(&fake, b"#!/bin/sh\n").unwrap();
        // SAFETY: single-threaded test; scoped env mutation.
        unsafe { std::env::set_var("NANOBPMN_NODE_BIN", &fake) };
        assert_eq!(find_node(), Some(fake));
        unsafe { std::env::set_var("NANOBPMN_NODE_BIN", "/nonexistent/definitely/not/node") };
        // Falls through to PATH/HOME; just assert it doesn't return the bogus path.
        assert_ne!(
            find_node(),
            Some(std::path::PathBuf::from("/nonexistent/definitely/not/node"))
        );
        unsafe { std::env::remove_var("NANOBPMN_NODE_BIN") };
        std::fs::remove_dir_all(&dir).ok();
    }
}
