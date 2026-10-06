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
//! - Appends go through a channel bounded **by bytes**
//!   (`NANOBPMN_TRACE_FILE_QUEUE_BYTES`, default 16 MiB) to a dedicated writer
//!   thread, so a slow disk never blocks the exporter and a stalled disk can
//!   never retain more than the configured byte budget of serialized traces —
//!   the memory guarantee this sink exists to provide. If the budget is full
//!   the trace is counted as *dropped* (a metric) rather than applying
//!   backpressure.
//! - The writer buffers and flushes on an **absolute** periodic deadline
//!   (`NANOBPMN_TRACE_FILE_FLUSH_MS`, default 1000 ms) that does not move when
//!   new traces arrive. There is no fsync per trace — losing the last flush
//!   interval on a crash is acceptable for analysis data.
//! - Optional size-based rotation (`NANOBPMN_TRACE_FILE_MAX_BYTES`, keeping
//!   `NANOBPMN_TRACE_FILE_KEEP` files). With rotation unset, the file is a plain
//!   append log you can hand to `logrotate` — but the writer opens the file
//!   once and never reopens it, so `logrotate` **must** use `copytruncate`;
//!   rename/create rotation would leave the writer appending to the renamed
//!   file while the new active path stays empty.
//! - Write/flush failures (e.g. a full disk) are counted as *errors* and
//!   logged; the affected line is discarded, never propagated back onto the
//!   engine's path.

use std::fs::{self, File, OpenOptions};
use std::io::{BufWriter, Write};
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::mpsc::{Receiver, RecvTimeoutError, SyncSender, TrySendError, sync_channel};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const DEFAULT_KEEP: usize = 5;
/// Default queue budget in bytes (16 MiB). Bounds the serialized-trace bytes a
/// stalled disk can retain, so a backlog can never balloon memory the way an
/// unbounded (or trace-count-bounded) queue could: one captured trace can carry
/// ~1024 × 16 KiB of stimulus payload, so a count-only bound is not a memory
/// bound at all.
const DEFAULT_QUEUE_BYTES: usize = 16 * 1024 * 1024;
/// Hard cap on the number of queued messages regardless of bytes, so a flood of
/// tiny traces cannot grow the channel's per-message bookkeeping without bound.
/// Generous enough that the byte budget is the operative bound in practice.
const MAX_QUEUE_MSGS: usize = 65536;
const DEFAULT_FLUSH_MS: u64 = 1000;

/// Resolved configuration for the NDJSON sink.
#[derive(Clone)]
pub(crate) struct SinkConfig {
    /// Destination file. Finished traces are appended here, one per line.
    pub path: PathBuf,
    /// Rotate once the active file reaches this many bytes. `None` disables
    /// in-process rotation (append-only; leave rotation to `logrotate` — which
    /// must then use `copytruncate`, since the writer never reopens the file).
    pub max_bytes: Option<u64>,
    /// Number of rotated files to keep (`<path>.1` … `<path>.<keep>`).
    pub keep: usize,
    /// Bounded append-channel budget **in bytes** of serialized trace. A full
    /// budget drops traces (a metric) rather than back-pressuring the engine.
    pub queue_bytes: usize,
    /// How often the writer flushes its buffer, as an absolute deadline that
    /// does not move when new traces arrive.
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
        // Accept the new byte-budget name, falling back to the legacy
        // trace-count name (reinterpreted as bytes) so an existing deployment
        // does not silently lose its bound. Default 16 MiB.
        let queue_bytes = parse_usize("NANOBPMN_TRACE_FILE_QUEUE_BYTES")
            .or_else(|| parse_usize("NANOBPMN_TRACE_FILE_QUEUE"))
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_QUEUE_BYTES);
        let flush_ms = parse_u64("NANOBPMN_TRACE_FILE_FLUSH_MS")
            .filter(|n| *n > 0)
            .unwrap_or(DEFAULT_FLUSH_MS);
        let tail = parse_usize("NANOBPMN_TRACE_FILE_TAIL").unwrap_or(0);
        Some(Self {
            path: PathBuf::from(path),
            max_bytes,
            keep,
            queue_bytes,
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
    /// Bytes of serialized trace currently sitting in the channel, shared with
    /// the writer (which subtracts on receipt). This is the *real* memory bound:
    /// the channel is count-capped too, but the byte budget is what guarantees a
    /// stalled disk cannot balloon memory.
    queued_bytes: Arc<AtomicU64>,
    queue_budget: u64,
    dropped: Arc<AtomicU64>,
    written: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
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
        // The channel is count-capped as a defensive ceiling on per-message
        // bookkeeping; the byte budget (`queued_bytes`) is the operative bound.
        let (tx, rx) = sync_channel::<Msg>(MAX_QUEUE_MSGS);
        let queued_bytes = Arc::new(AtomicU64::new(0));
        let dropped = Arc::new(AtomicU64::new(0));
        let written = Arc::new(AtomicU64::new(0));
        let errors = Arc::new(AtomicU64::new(0));
        let tail = cfg.tail;
        let queue_budget = cfg.queue_bytes as u64;
        let written_w = written.clone();
        let errors_w = errors.clone();
        let queued_w = queued_bytes.clone();
        let handle = std::thread::Builder::new()
            .name("trace-ndjson".to_string())
            .spawn(move || writer_loop(cfg, rx, queued_w, written_w, errors_w))
            .expect("spawn trace-ndjson writer thread");
        Self {
            tx,
            queued_bytes,
            queue_budget,
            dropped,
            written,
            errors,
            handle: Some(handle),
            tail,
        }
    }

    /// Enqueues one finished-trace NDJSON line. Never blocks: if the byte budget
    /// (or the defensive channel cap) is full, or the writer is gone, the trace
    /// is counted as dropped rather than back-pressuring the caller.
    pub(crate) fn append(&self, line: String) {
        let bytes = line.len() as u64 + 1; // + the newline the writer adds
        // Reserve budget before sending so a stalled disk cannot accumulate more
        // than `queue_budget` bytes of pending traces. `fetch_add` then check:
        // if we overflowed the budget, undo the reservation and drop.
        let prev = self.queued_bytes.fetch_add(bytes, Ordering::AcqRel);
        if prev + bytes > self.queue_budget {
            self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
            self.dropped.fetch_add(1, Ordering::Relaxed);
            return;
        }
        match self.tx.try_send(Msg::Line(line)) {
            Ok(()) => {}
            Err(TrySendError::Full(_)) | Err(TrySendError::Disconnected(_)) => {
                // Release the reservation: the writer will never see this line.
                self.queued_bytes.fetch_sub(bytes, Ordering::AcqRel);
                self.dropped.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    /// Number of finished traces dropped because the byte budget (or the
    /// defensive channel cap) was full.
    pub(crate) fn dropped(&self) -> u64 {
        self.dropped.load(Ordering::Relaxed)
    }

    /// Number of finished traces whose bytes have reached the OS (survived a
    /// successful flush). A trace accepted into the buffer but not yet flushed
    /// is not counted here, so this never over-reports durability.
    pub(crate) fn written(&self) -> u64 {
        self.written.load(Ordering::Relaxed)
    }

    /// Number of finished traces lost to a write/flush error (e.g. a full disk)
    /// after they reached the writer thread, plus one for a flush operation that
    /// failed with an empty buffer (no trace lost, but the disk fault is still
    /// worth a metric). Distinct from [`Self::dropped`], which counts traces
    /// that never reached the writer.
    pub(crate) fn errors(&self) -> u64 {
        self.errors.load(Ordering::Relaxed)
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
///
/// Returns `Ok(())` only when the active file was successfully renamed to
/// `<path>.1` — the caller resets its tracked size to zero only then. A failure
/// to remove an aged-out segment or shift an intermediate one is logged but
/// non-fatal (retention is best-effort); a failure to rename the *active* file
/// is returned as an `Err` so the caller keeps the real (over-limit) size and
/// counts the error rather than silently defeating the configured bound.
fn rotate(path: &Path, keep: usize) -> std::io::Result<()> {
    let s = path.to_string_lossy();
    // Drop the oldest beyond the retention window (best-effort).
    if let Err(e) = fs::remove_file(format!("{s}.{keep}"))
        && e.kind() != std::io::ErrorKind::NotFound
    {
        eprintln!("nano-trace-store: cannot remove aged-out trace file {s}.{keep}: {e}");
    }
    for i in (1..keep).rev() {
        let from = PathBuf::from(format!("{s}.{i}"));
        if from.exists()
            && let Err(e) = fs::rename(&from, PathBuf::from(format!("{s}.{}", i + 1)))
        {
            eprintln!(
                "nano-trace-store: cannot shift trace file {s}.{i} to .{}: {e}",
                i + 1
            );
        }
    }
    // The load-bearing step: rename the active file. Propagate a failure so the
    // caller keeps the real size and counts the error.
    fs::rename(path, PathBuf::from(format!("{s}.1")))
}

/// The writer thread body: drains the channel, appends lines, flushes on an
/// **absolute** periodic deadline, and rotates on size. Terminates on `Shutdown`
/// or when the sender is dropped, flushing first. Write/flush failures are
/// counted in `errors` and logged once per line — the line is then discarded (a
/// durability sink must never propagate an error back onto the engine's path).
///
/// Accounting: `written` counts a trace only once its bytes have survived a
/// successful `flush` (i.e. reached the OS). Lines accepted into the `BufWriter`
/// but not yet flushed are `pending`; a failed flush moves the whole pending
/// batch into `errors`, because `BufWriter` drops its buffer on a failed flush
/// and those lines are genuinely lost.
fn writer_loop(
    cfg: SinkConfig,
    rx: Receiver<Msg>,
    queued_bytes: Arc<AtomicU64>,
    written: Arc<AtomicU64>,
    errors: Arc<AtomicU64>,
) {
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
                if let Msg::Line(line) = msg {
                    queued_bytes.fetch_sub(line.len() as u64 + 1, Ordering::AcqRel);
                    errors.fetch_add(1, Ordering::Relaxed);
                } else {
                    break; // Shutdown
                }
            }
            return;
        }
    };
    let mut size = file_size(&cfg.path);
    let mut writer = BufWriter::new(file);
    // Lines accepted into the BufWriter but not yet flushed to the OS.
    let mut pending: u64 = 0;
    // Absolute flush deadline: set once and advanced only when a flush actually
    // happens, so a steady stream of arrivals (each restarting a relative
    // timeout) can never starve the flush. This is what makes the documented
    // "a crash loses at most one flush interval" guarantee hold.
    let mut next_flush = Instant::now() + cfg.flush_interval;

    // Flushes the buffer and settles the pending accounting. On success the
    // pending lines become `written`; on failure they become `errors` (the
    // BufWriter has dropped them). When `reset_deadline` is true the absolute
    // deadline advances so a persistently-failing disk is not retried in a tight
    // loop; the terminal Shutdown/Disconnected flushes pass false because the
    // deadline is never read again.
    let do_flush = |writer: &mut BufWriter<File>,
                    pending: &mut u64,
                    next_flush: &mut Instant,
                    reset_deadline: bool| {
        match writer.flush() {
            Ok(()) => {
                written.fetch_add(*pending, Ordering::Relaxed);
            }
            Err(e) => {
                errors.fetch_add((*pending).max(1), Ordering::Relaxed);
                eprintln!(
                    "nano-trace-store: flush of trace file {} failed: {e}",
                    cfg.path.display()
                );
            }
        }
        *pending = 0;
        if reset_deadline {
            *next_flush = Instant::now() + cfg.flush_interval;
        }
    };

    loop {
        // Wait only until the absolute deadline; if it has already passed,
        // flush immediately rather than blocking on another arrival.
        let wait = next_flush.saturating_duration_since(Instant::now());
        match rx.recv_timeout(wait) {
            Ok(Msg::Line(line)) => {
                let line_bytes = line.len() as u64 + 1;
                // Prospective rotation: rotate when appending this line would
                // push the active file over the cap — unless the file is empty,
                // in which case a single oversized line goes into a fresh
                // segment rather than spinning on rotation forever.
                if let Some(max) = cfg.max_bytes
                    && size > 0
                    && size + line_bytes > max
                {
                    // Flush pending bytes into the about-to-be-renamed segment so
                    // they are accounted and durable before rotation.
                    do_flush(&mut writer, &mut pending, &mut next_flush, true);
                    drop(writer);
                    match rotate(&cfg.path, cfg.keep) {
                        Ok(()) => {
                            // The active file is gone (renamed); reopen a fresh
                            // empty segment and reset the tracked size to zero.
                            match open_append(&cfg.path) {
                                Ok(f) => {
                                    size = 0;
                                    writer = BufWriter::new(f);
                                }
                                Err(e) => {
                                    errors.fetch_add(1, Ordering::Relaxed);
                                    eprintln!(
                                        "nano-trace-store: cannot reopen trace file {} after rotation: {e}",
                                        cfg.path.display()
                                    );
                                    return;
                                }
                            }
                        }
                        Err(e) => {
                            // Rotation failed (e.g. permission): keep the real
                            // over-limit size, count the error, and continue
                            // appending to a reopened handle on the still-present
                            // file so the size bound is not silently defeated.
                            errors.fetch_add(1, Ordering::Relaxed);
                            eprintln!(
                                "nano-trace-store: rotation of trace file {} failed, continuing to append: {e}",
                                cfg.path.display()
                            );
                            match open_append(&cfg.path) {
                                Ok(f) => {
                                    writer = BufWriter::new(f);
                                    size = file_size(&cfg.path);
                                }
                                Err(_) => return,
                            }
                        }
                    }
                }
                match writer
                    .write_all(line.as_bytes())
                    .and_then(|()| writer.write_all(b"\n"))
                {
                    Ok(()) => {
                        size += line_bytes;
                        pending += 1;
                    }
                    Err(e) => {
                        // The line is lost (e.g. disk full): count and log it so
                        // silent trace loss is observable, but keep serving —
                        // the next write may succeed and the engine must never
                        // see this error.
                        errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "nano-trace-store: write to trace file {} failed, trace discarded: {e}",
                            cfg.path.display()
                        );
                    }
                }
                queued_bytes.fetch_sub(line_bytes, Ordering::AcqRel);
            }
            Ok(Msg::Shutdown) => {
                do_flush(&mut writer, &mut pending, &mut next_flush, false);
                break;
            }
            Err(RecvTimeoutError::Timeout) => {
                // Absolute deadline reached.
                do_flush(&mut writer, &mut pending, &mut next_flush, true);
            }
            Err(RecvTimeoutError::Disconnected) => {
                do_flush(&mut writer, &mut pending, &mut next_flush, false);
                break;
            }
        }
    }
}
