//! Optional NDJSON durability sink for finished traces (issue #1343).
//!
//! The Tier-A [`crate::TraceStore`] is an in-memory ring: everything is lost on
//! restart, and finished instances are evicted as new ones arrive. With
//! recorded-input capture on, the ring's memory also grows with payload size, so
//! capture can't be left on in production and replay has no durable history.
//!
//! This sink is the opt-in fix. When set via `NANOBPMN_TRACE_FILE=<path>`, each
//! **finished** instance (completed, terminated or cancelled) is appended to the
//! file as exactly one NDJSON line — the same JSON shape as
//! `GET /console/api/traces/{key}` — then dropped from the ring. The ring then
//! holds only active instances (plus an optional small tail of recently finished
//! ones for the console, `NANOBPMN_TRACE_FILE_TAIL`), so memory is bounded by the
//! live set and capture can stay on.
//!
//! Durability is deliberately cheap, never on the engine's path:
//! - Appends go through a **bounded** channel to a dedicated writer thread, so a
//!   slow disk never blocks the exporter. If the channel is full the trace is
//!   counted as *dropped* (a metric) rather than applying backpressure.
//! - The writer buffers and flushes on a periodic interval. There is no fsync per
//!   trace — losing the last flush interval on a crash is acceptable for analysis
//!   data.
//! - Optional size-based rotation (`NANOBPMN_TRACE_FILE_MAX_BYTES`, keeping
//!   `NANOBPMN_TRACE_FILE_KEEP` files). With rotation unset, the file is a plain
//!   append log you can hand to `logrotate`.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::thread::JoinHandle;
use std::time::Duration;

const DEFAULT_KEEP: usize = 5;
const DEFAULT_QUEUE: usize = 4096;
const DEFAULT_FLUSH_MS: u64 = 1000;

/// Resolved configuration for the NDJSON sink.
#[derive(Clone)]
pub(crate) struct SinkConfig {
    /// Destination file. Finished traces are appended here, one per line.
    pub path: PathBuf,
    /// Rotate once the active file reaches this many bytes. `None` disables
    /// in-process rotation (append-only; leave rotation to `logrotate`).
    pub max_bytes: Option<u64>,
    /// Number of rotated files to keep (`<path>.1` … `<path>.<keep>`).
    pub keep: usize,
    /// Bounded append-channel depth. A full channel drops traces (a metric)
    /// rather than back-pressuring the engine.
    pub queue: usize,
    /// How often the writer flushes its buffer when idle.
    pub flush_interval: Duration,
    /// How many most-recently-finished instances to keep in the in-memory ring
    /// for the console after they are written to the file. `0` (the default)
    /// removes a finished instance from the ring immediately.
    pub tail: usize,
}

impl SinkConfig {
    /// Resolves the sink configuration from the environment. Returns `None` when
    /// `NANOBPMN_TRACE_FILE` is unset or blank — the default, no-behaviour-change
    /// path.
    pub(crate) fn from_env() -> Option<Self> {
        let path = std::env::var("NANOBPMN_TRACE_FILE")
            .ok()
            .map(|p| p.trim().to_string())
            .filter(|p| !p.is_empty())?;
        let max_bytes = parse_u64("NANOBPMN_TRACE_FILE_MAX_BYTES").filter(|n| *n > 0);
        let keep = parse_usize("NANOBPMN_TRACE_FILE_KEEP")
            .unwrap_or(DEFAULT_KEEP)
            .max(1);
        let queue = parse_usize("NANOBPMN_TRACE_FILE_QUEUE")
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_QUEUE);
        let flush_ms = parse_u64("NANOBPMN_TRACE_FILE_FLUSH_MS")
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_FLUSH_MS);
        let tail = parse_usize("NANOBPMN_TRACE_FILE_TAIL").unwrap_or(0);
        Some(Self {
            path: PathBuf::from(path),
            max_bytes,
            keep,
            queue,
            flush_interval: Duration::from_millis(flush_ms),
            tail,
        })
    }
}

fn parse_u64(name: &str) -> Option<u64> {
    std::env::var(name).ok()?.trim().parse::<u64>().ok()
}

fn parse_usize(name: &str) -> Option<usize> {
    std::env::var(name).ok()?.trim().parse::<usize>().ok()
}

/// A message to the writer thread.
enum Msg {
    /// One serialized NDJSON trace line (without a trailing newline).
    Line(String),
    /// Flush and shut down (sent on drop).
    Shutdown,
}

/// The write side of the NDJSON sink: a handle onto a dedicated writer thread.
///
/// Cloning the counters is cheap (`Arc`), but the sink itself is not `Clone`:
/// exactly one writer thread owns the file. Dropping the sink flushes and joins
/// the writer so a clean shutdown never loses buffered traces.
pub(crate) struct TraceSink {
    tx: SyncSender<Msg>,
    dropped: Arc<AtomicU64>,
    written: Arc<AtomicU64>,
    handle: Option<JoinHandle<()>>,
    tail: usize,
}

impl TraceSink {
    /// Builds the sink from the environment, or `None` when `NANOBPMN_TRACE_FILE`
    /// is unset — preserving today's in-memory-only behaviour.
    pub(crate) fn from_env() -> Option<Self> {
        SinkConfig::from_env().map(Self::spawn)
    }

    /// Spawns the writer thread for an explicit configuration (used by tests).
    pub(crate) fn spawn(cfg: SinkConfig) -> Self {
        let (tx, rx) = sync_channel::<Msg>(cfg.queue);
        let dropped = Arc::new(AtomicU64::new(0));
        let written = Arc::new(AtomicU64::new(0));
        let tail = cfg.tail;
        let written_w = written.clone();
        let handle = std::thread::Builder::new()
            .name("trace-ndjson".to_string())
            .spawn(move || writer_loop(cfg, rx, written_w))
            .expect("spawn trace-ndjson writer thread");
        Self {
            tx,
            dropped,
            written,
            handle: Some(handle),
            tail,
        }
    }

    /// Enqueues one finished-trace NDJSON line. Never blocks: if the bounded
    /// channel is full (or the writer is gone), the trace is counted as dropped
    /// rather than back-pressuring the caller.
    pub(crate) fn append(&self, line: String) {
        match self.tx.try_send(Msg::Line(line)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Number of finished traces dropped because the channel was full.
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Number of finished traces written to the file.
    pub(crate) fn written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    /// How many recently-finished instances to retain in the ring for the
    /// console after writing them to the file.
    pub(crate) fn tail(&self) -> usize {
        self.tail
    }
}

impl Drop for TraceSink {
    fn drop(&mut self) {
        // Best-effort clean shutdown: ask the writer to flush, then join so
        // buffered traces reach disk before the process exits.
        let _ = self.tx.send(Msg::Shutdown);
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}

/// Opens `path` for appending, creating it (and any missing parent directory)
/// if necessary.
fn open_append(path: &Path) -> std::io::Result<File> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    OpenOptions::new().create(true).append(true).open(path)
}

/// Current size of `path` in bytes, or `0` if it does not yet exist.
fn file_size(path: &Path) -> u64 {
    fs::metadata(path).map(|m| m.len()).unwrap_or(0)
}

/// Rotates `path` → `path.1`, shifting existing `path.<n>` up by one and keeping
/// at most `keep` rotated files (`logrotate`-style).
fn rotate(path: &Path, keep: usize) {
    let s = path.to_string_lossy();
    // Drop the oldest beyond the retention window.
    let _ = fs::remove_file(format!("{s}.{keep}"));
    for i in (1..keep).rev() {
        let from = PathBuf::from(format!("{s}.{i}"));
        if from.exists() {
            let _ = fs::rename(&from, PathBuf::from(format!("{s}.{}", i + 1)));
        }
    }
    let _ = fs::rename(path, PathBuf::from(format!("{s}.1")));
}

/// The writer thread body: drains the channel, appends lines, flushes on a
/// periodic interval, and rotates on size. Terminates on `Shutdown` or when the
/// sender is dropped, flushing first.
fn writer_loop(cfg: SinkConfig, rx: std::sync::mpsc::Receiver<Msg>, written: Arc<AtomicU64>) {
    let file = match open_append(&cfg.path) {
        Ok(f) => f,
        Err(e) => {
            eprintln!(
                "nano-trace-store: cannot open trace file {}: {e}",
                cfg.path.display()
            );
            // Drain so senders see the channel as alive-but-dropping rather than
            // blocking forever; traces are simply discarded.
            while let Ok(msg) = rx.recv() {
                if matches!(msg, Msg::Shutdown) {
                    break;
                }
            }
            return;
        }
    };
    let mut size = file_size(&cfg.path);
    let mut writer = BufWriter::new(file);

    loop {
        match rx.recv_timeout(cfg.flush_interval) {
            Ok(Msg::Line(line)) => {
                // Rotate before writing when the active file is already at the
                // cap, so each rotated segment stays under the limit.
                if let Some(max) = cfg.max_bytes
                    && size >= max
                {
                    let _ = writer.flush();
                    drop(writer);
                    rotate(&cfg.path, cfg.keep);
                    match open_append(&cfg.path) {
                        Ok(f) => {
                            size = 0;
                            writer = BufWriter::new(f);
                        }
                        Err(e) => {
                            eprintln!(
                                "nano-trace-store: cannot reopen trace file {} after rotation: {e}",
                                cfg.path.display()
                            );
                            return;
                        }
                    }
                }
                if writer.write_all(line.as_bytes()).is_ok() && writer.write_all(b"\n").is_ok() {
                    size += line.len() as u64 + 1;
                    written.fetch_add(1, Ordering::Relaxed);
                }
            }
            Ok(Msg::Shutdown) => {
                let _ = writer.flush();
                break;
            }
            Err(RecvTimeoutError::Timeout) => {
                let _ = writer.flush();
            }
            Err(RecvTimeoutError::Disconnected) => {
                let _ = writer.flush();
                break;
            }
        }
    }
}
