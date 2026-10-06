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
//!   backpressure — except that a single trace larger than the whole budget is
//!   still admitted to an *empty* queue (the budget bounds backlog, not one
//!   trace's size; a healthy writer drains it immediately).
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
    /// stalled disk cannot balloon memory. Visible to the crate so tests can
    /// pin a reservation (simulating a stalled writer) deterministically.
    pub(crate) queued_bytes: Arc<AtomicU64>,
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
    /// is counted as dropped rather than back-pressuring the caller. A single
    /// line larger than the whole budget is admitted when the queue is empty —
    /// the same lone-oversized exception the writer's rotation path makes —
    /// because the budget bounds *backlog*, not one trace's size: a healthy
    /// writer drains the line immediately, so it never becomes backlog.
    pub(crate) fn append(&self, line: String) {
        let bytes = line.len() as u64 + 1; // + the newline the writer adds
        // Reserve budget before sending so a stalled disk cannot accumulate more
        // than `queue_budget` bytes of pending traces. `fetch_add` then check:
        // if we overflowed a *non-empty* queue, undo the reservation and drop.
        let prev = self.queued_bytes.fetch_add(bytes, Ordering::AcqRel);
        if prev != 0 && prev + bytes > self.queue_budget {
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
    /// after they reached the writer thread: a line the `BufWriter` rejected, or
    /// a line still buffered when the writer was abandoned (dropped for rotation
    /// or on thread exit) after its flush failed. A flush that fails but leaves
    /// the bytes buffered for a later retry is *not* counted here until the
    /// writer is actually abandoned. Distinct from [`Self::dropped`], which
    /// counts traces that never reached the writer.
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
/// to remove an aged-out segment is logged but non-fatal (retention is
/// best-effort). A failure to *shift* an intermediate segment is **fatal to the
/// rotation**: on platforms where `rename` replaces an existing destination,
/// renaming the active file onto a still-present `.1` would overwrite the newest
/// rotated segment, losing it in addition to the intended aged-out file. So any
/// shift failure aborts before the active file is touched, and a failure to
/// rename the *active* file is likewise returned as an `Err` — the caller keeps
/// the real (over-limit) size and counts the error rather than silently
/// defeating the configured bound.
pub(crate) fn rotate(path: &Path, keep: usize) -> std::io::Result<()> {
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
            // Do not rename the active file onto a segment we failed to move:
            // that would overwrite the newest rotated segment. Surface the
            // failure so the caller keeps the real size and counts the error.
            return Err(std::io::Error::new(
                e.kind(),
                format!("cannot shift trace file {s}.{i} to .{}: {e}", i + 1),
            ));
        }
    }
    // The load-bearing step: rename the active file. Propagate a failure so the
    // caller keeps the real size and counts the error.
    fs::rename(path, PathBuf::from(format!("{s}.1")))
}

/// Settles pending-line accounting for a single flush outcome and returns the
/// new pending count. The **single source of truth** for how a flush transitions
/// `pending`/`written`:
///
/// - On success the pending lines have reached the OS, so they are counted in
///   `written` once and pending resets to `0`.
/// - On failure the lines stay buffered — a `BufWriter` retains (does not drop)
///   the bytes it could not write — so pending is preserved for a later retry
///   and **nothing is counted lost here**. Counting them as `errors` now would
///   both double-count them (a later successful flush writes and `written`-counts
///   the same bytes) and under-report `written`.
fn settle_flush(flush_ok: bool, pending: u64, written: &AtomicU64) -> u64 {
    if flush_ok {
        written.fetch_add(pending, Ordering::Relaxed);
        0
    } else {
        pending
    }
}

/// Accounts lines still buffered when a writer is abandoned (dropped for
/// rotation, or on thread exit) and returns the new pending count (`0`). A prior
/// flush already failed to persist these bytes and dropping the `BufWriter`
/// cannot, so they are genuinely lost now and counted in `errors` exactly once.
fn account_lost_pending(pending: u64, errors: &AtomicU64) -> u64 {
    if pending > 0 {
        errors.fetch_add(pending, Ordering::Relaxed);
    }
    0
}

/// Drains every message still queued in `rx`, releasing each line's byte
/// reservation and counting it as an error, until a `Shutdown` (or the sender
/// disconnecting) ends the stream. Used when the writer can no longer persist —
/// the file failed to open or reopen — so senders see the channel as
/// alive-but-dropping rather than blocking forever, and no queued trace is lost
/// from the `queued_bytes`/`errors` accounting.
fn drain_and_account(rx: &Receiver<Msg>, queued_bytes: &AtomicU64, errors: &AtomicU64) {
    while let Ok(msg) = rx.recv() {
        if let Msg::Line(line) = msg {
            queued_bytes.fetch_sub(line.len() as u64 + 1, Ordering::AcqRel);
            errors.fetch_add(1, Ordering::Relaxed);
        } else {
            break; // Shutdown
        }
    }
}

/// The writer thread body: drains the channel, appends lines, flushes on an
/// **absolute** periodic deadline, and rotates on size. Terminates on `Shutdown`
/// or when the sender is dropped, flushing first. Write/flush failures are
/// counted in `errors` and logged once per line — the line is then discarded (a
/// durability sink must never propagate an error back onto the engine's path).
///
/// Accounting: `written` counts a trace only once its bytes have survived a
/// successful `flush` (i.e. reached the OS). Lines accepted into the `BufWriter`
/// but not yet flushed are `pending`; a failed flush leaves them buffered (a
/// `BufWriter` retains, not drops, the bytes it could not write), so they stay
/// `pending` and are retried on the next flush. They are counted in `errors`
/// only when the writer is abandoned (dropped for rotation or on thread exit)
/// without having flushed them, which is the point at which they are truly lost.
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
            // blocking forever; traces are simply discarded (and accounted).
            drain_and_account(&rx, &queued_bytes, &errors);
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

    // Flushes the buffer and settles the pending accounting. On a successful
    // flush the pending lines have reached the OS and become `written`. On a
    // FAILED flush the lines stay buffered: `BufWriter` retains the bytes it
    // could not write (it does not drop the whole buffer), so a later flush may
    // still persist them. We therefore keep `pending` intact and do NOT count
    // the batch as `errors` here — doing so would both double-count the lines (a
    // later successful flush writes and `written`-counts the same bytes) and
    // under-report `written`. Buffered lines are classified as lost only when the
    // writer is abandoned (see `account_lost_pending`). When `reset_deadline` is
    // true the absolute deadline advances so a persistently-failing disk is not
    // retried in a tight loop; the terminal Shutdown/Disconnected flushes pass
    // false because the deadline is never read again.
    let do_flush = |writer: &mut BufWriter<File>,
                    pending: &mut u64,
                    next_flush: &mut Instant,
                    reset_deadline: bool| {
        match writer.flush() {
            Ok(()) => {
                *pending = settle_flush(true, *pending, &written);
            }
            Err(e) => {
                *pending = settle_flush(false, *pending, &written);
                eprintln!(
                    "nano-trace-store: flush of trace file {} failed: {e}",
                    cfg.path.display()
                );
            }
        }
        if reset_deadline {
            *next_flush = Instant::now() + cfg.flush_interval;
        }
    };

    // Accounts lines still buffered when a writer is about to be abandoned
    // (dropped for rotation, or on thread exit). Dropping a `BufWriter` cannot
    // reliably persist bytes a prior flush already failed to write, so those
    // lines are genuinely lost now and counted as `errors` exactly once.
    let account_lost = |pending: &mut u64| {
        *pending = account_lost_pending(*pending, &errors);
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
                    // Any lines still pending survived a failed flush; the writer
                    // we are about to drop cannot persist them, so they are lost.
                    account_lost(&mut pending);
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
                                    // The writer can no longer persist. Drain and
                                    // account every already-queued line (release
                                    // its byte reservation, count it lost) rather
                                    // than returning with the channel silently
                                    // dropping them and the reservations leaking.
                                    drain_and_account(&rx, &queued_bytes, &errors);
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
                                Err(e) => {
                                    // Cannot continue appending either. Drain and
                                    // account the queued lines (release their byte
                                    // reservations, count them lost) before exit.
                                    errors.fetch_add(1, Ordering::Relaxed);
                                    eprintln!(
                                        "nano-trace-store: cannot reopen trace file {} after failed rotation: {e}",
                                        cfg.path.display()
                                    );
                                    drain_and_account(&rx, &queued_bytes, &errors);
                                    return;
                                }
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
    // The thread is exiting: the final flush above may have failed, leaving lines
    // buffered in the writer we are about to drop. They cannot be persisted now,
    // so classify them as lost (counted once).
    account_lost(&mut pending);
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{account_lost_pending, settle_flush};

    fn load(a: &AtomicU64) -> u64 {
        a.load(Ordering::Relaxed)
    }

    #[test]
    fn successful_flush_counts_pending_as_written_once() {
        let written = AtomicU64::new(0);
        let pending = settle_flush(true, 3, &written);
        assert_eq!(pending, 0, "a successful flush clears pending");
        assert_eq!(
            load(&written),
            3,
            "all pending lines are written exactly once"
        );
    }

    #[test]
    fn failed_flush_keeps_pending_and_counts_nothing() {
        // Regression for the lost-accounting finding: a BufWriter retains the
        // bytes it could not flush, so a failed flush must NOT count the batch as
        // written and must NOT drop/zero pending — the lines are retried.
        let written = AtomicU64::new(0);
        let pending = settle_flush(false, 3, &written);
        assert_eq!(pending, 3, "a failed flush preserves pending for retry");
        assert_eq!(load(&written), 0, "nothing is written on a failed flush");
    }

    #[test]
    fn failed_then_successful_flush_writes_each_line_exactly_once() {
        // The exact double-accounting the finding warned about: after a failed
        // flush retains the bytes, the next successful flush persists them — they
        // must be counted in `written` once and never in `errors`.
        let written = AtomicU64::new(0);
        let errors = AtomicU64::new(0);
        let pending = settle_flush(false, 2, &written); // flush fails, bytes buffered
        assert_eq!(pending, 2);
        let pending = settle_flush(true, pending, &written); // retry succeeds
        assert_eq!(pending, 0);
        assert_eq!(
            load(&written),
            2,
            "the retried lines are written exactly once"
        );
        assert_eq!(
            load(&errors),
            0,
            "nothing is counted lost when the retry persists them"
        );
    }

    #[test]
    fn abandoning_buffered_lines_counts_them_lost_once() {
        // When the writer is abandoned (dropped for rotation or on thread exit)
        // with lines still buffered after a failed flush, they are truly lost and
        // counted in `errors` exactly once.
        let written = AtomicU64::new(0);
        let errors = AtomicU64::new(0);
        let pending = settle_flush(false, 4, &written); // flush fails, 4 buffered
        let pending = account_lost_pending(pending, &errors); // writer abandoned
        assert_eq!(pending, 0);
        assert_eq!(load(&written), 0);
        assert_eq!(
            load(&errors),
            4,
            "abandoned buffered lines are lost exactly once"
        );
    }

    #[test]
    fn abandoning_with_no_pending_counts_nothing() {
        // A clean abandon (everything already flushed) must not fabricate an
        // error — the old `.max(1)` behaviour over-counted an empty buffer.
        let errors = AtomicU64::new(0);
        let pending = account_lost_pending(0, &errors);
        assert_eq!(pending, 0);
        assert_eq!(load(&errors), 0, "no pending means no loss to account");
    }
}
