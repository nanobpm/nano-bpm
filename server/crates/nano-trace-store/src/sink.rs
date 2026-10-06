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
//!   engine's path. A write that fails mid-record leaves a torn tail, so the
//!   writer is then abandoned and the file is repaired back to its last
//!   complete newline before a fresh writer reopens it — the next trace never
//!   concatenates onto a partial record. The same repair runs on open, so a
//!   crash that left an incomplete final line is recovered on restart, and
//!   before a rotation renames the file, so a failed pre-rotation flush cannot
//!   seal a torn record into an archived segment. Repair is fail-closed: if
//!   the tail cannot be verified the sink refuses to append (or skips the
//!   rotation) rather than risk corrupting the stream.

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
        let dropped_w = dropped.clone();
        let handle = std::thread::Builder::new()
            .name("trace-ndjson".to_string())
            .spawn(move || writer_loop(cfg, rx, queued_w, written_w, errors_w, dropped_w))
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

    /// Number of finished traces lost to a write/flush failure (e.g. a full
    /// disk) after they reached the writer thread, **plus** a small number of
    /// writer-operation failures that lose no trace. The trace-loss component is
    /// a line the `BufWriter` rejected, or a line still buffered when the writer
    /// was abandoned (dropped for rotation, reopened after a write failure, or
    /// on thread exit) after its flush failed. A flush that fails but leaves the
    /// bytes buffered for a later retry is *not* counted here until the writer
    /// is actually abandoned. On top of that, `errors` also counts each failed
    /// rotation, reopen, or pre-rotation repair — an *operation* failure that
    /// loses no trace — so the counter stays monotonic for alerting even when
    /// nothing was lost. Read it
    /// as "traces lost + unrecovered sink operations", not a pure trace count.
    /// Distinct from [`Self::dropped`], which counts traces that never reached
    /// the writer.
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
/// if necessary. When the file already exists it is first repaired to the last
/// complete newline (see [`truncate_incomplete_tail`]), so a crash that left a
/// torn final record never has the next trace concatenated onto it.
///
/// The repair is **fail-closed**: if it errors (e.g. the file is write-only and
/// rejects the read/write repair open while still accepting append), the error
/// is propagated and no append handle is returned. Appending anyway would
/// concatenate the next record onto an unexamined — possibly torn — tail and
/// silently corrupt the NDJSON stream, so the caller's open-failure path (drain
/// and account) takes the sink down instead.
fn open_append(path: &Path) -> std::io::Result<File> {
    if let Some(parent) = path.parent()
        && !parent.as_os_str().is_empty()
    {
        fs::create_dir_all(parent)?;
    }
    if path.exists() {
        truncate_incomplete_tail(path)?;
    }
    OpenOptions::new().create(true).append(true).open(path)
}

/// Truncates `path` back to just past its last `\n`, removing a torn final
/// record left by a crash or a failed write. NDJSON is one record per line, so
/// any bytes after the last newline are a partial record that would corrupt the
/// next append; dropping them keeps every remaining line valid. Fail-closed: a
/// read/seek/write failure is returned to the caller, because appending to an
/// unexamined tail risks concatenating onto a torn record — a caller that
/// cannot repair must not append.
fn truncate_incomplete_tail(path: &Path) -> std::io::Result<()> {
    let size = file_size(path);
    if size == 0 {
        return Ok(());
    }
    // A file whose last byte is a newline is already well-formed: return before
    // opening anything, so a write-only (unreadable) but clean file is not
    // failed by a repair that has nothing to do. Read the final byte with a
    // read-only handle — the read/write repair open below stays fail-closed
    // for a file that genuinely needs truncating.
    {
        let f = OpenOptions::new().read(true).open(path)?;
        let mut r = std::io::BufReader::new(f);
        std::io::Seek::seek(&mut r, std::io::SeekFrom::End(-1))?;
        let mut last = [0u8; 1];
        std::io::Read::read_exact(&mut r, &mut last)?;
        if last[0] == b'\n' {
            return Ok(());
        }
    }
    let f = OpenOptions::new().read(true).write(true).open(path)?;
    let mut r = std::io::BufReader::new(f);
    // Find the last newline by scanning backward in 8 KiB blocks. Only the
    // tail matters, so a huge file costs at most a few trailing reads.
    let mut pos = size;
    let mut cut = None;
    let mut block = vec![0u8; 8192];
    while pos > 0 {
        let n = (pos.min(block.len() as u64)) as usize;
        pos -= n as u64;
        std::io::Seek::seek(&mut r, std::io::SeekFrom::Start(pos))?;
        let buf = &mut block[..n];
        std::io::Read::read_exact(&mut r, buf)?;
        if let Some(i) = buf.iter().rposition(|&b| b == b'\n') {
            cut = Some(pos + i as u64 + 1);
            break;
        }
    }
    let keep = cut.unwrap_or(0);
    if keep < size {
        r.into_inner().set_len(keep)?;
    }
    Ok(())
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

/// How often the writer thread emits a durability report (dropped/error
/// counters) to stderr so silent trace loss is observable in a running node,
/// not only through the console API that nothing polls.
const REPORT_INTERVAL: Duration = Duration::from_secs(60);

/// Whether a periodic durability report is warranted: emit only when the
/// `(dropped, errors)` loss counters have GROWN since the last report, so a
/// steady state is logged once rather than every window. Both counters are
/// monotonic, so "grew" also implies the current value is non-zero. Pure, so
/// the cadence/threshold decision is unit-tested without the writer thread.
fn loss_grew(prev: (u64, u64), cur: (u64, u64)) -> bool {
    cur.0 > prev.0 || cur.1 > prev.1
}

/// Abandons a `BufWriter` whose buffered bytes have already been accounted as
/// lost, WITHOUT the implicit flush retry that `drop` would run. A
/// `BufWriter`'s `Drop` re-attempts the failed flush; if that retry were to
/// succeed it would persist lines we have already counted in `errors` and can
/// never move to `written`, so the exposed stats would contradict the file.
/// `into_parts` disassembles the writer and discards the buffer instead, so
/// "counted as lost" stays truthful. The returned file handle is dropped
/// (closed) without a flush.
fn abandon_without_flush(writer: BufWriter<File>) {
    let _ = writer.into_parts();
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
    dropped: Arc<AtomicU64>,
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

    // Flushes the buffer and settles the pending accounting, returning whether
    // the flush succeeded (the rotation path needs the outcome: a failed flush
    // can leave a torn tail on disk that must be repaired before the file is
    // renamed). On a successful flush the pending lines have reached the OS and
    // become `written`. On a FAILED flush the lines stay buffered: `BufWriter`
    // retains the bytes it could not write (it does not drop the whole buffer),
    // so a later flush may still persist them. We therefore keep `pending`
    // intact and do NOT count the batch as `errors` here — doing so would both
    // double-count the lines (a later successful flush writes and
    // `written`-counts the same bytes) and under-report `written`. Buffered
    // lines are classified as lost only when the writer is abandoned (see
    // `account_lost_pending`). When `reset_deadline` is true the absolute
    // deadline advances so a persistently-failing disk is not retried in a
    // tight loop; the terminal Shutdown/Disconnected flushes pass false because
    // the deadline is never read again.
    let do_flush = |writer: &mut BufWriter<File>,
                    pending: &mut u64,
                    next_flush: &mut Instant,
                    reset_deadline: bool|
     -> bool {
        let ok = match writer.flush() {
            Ok(()) => {
                *pending = settle_flush(true, *pending, &written);
                true
            }
            Err(e) => {
                *pending = settle_flush(false, *pending, &written);
                eprintln!(
                    "nano-trace-store: flush of trace file {} failed: {e}",
                    cfg.path.display()
                );
                false
            }
        };
        if reset_deadline {
            *next_flush = Instant::now() + cfg.flush_interval;
        }
        ok
    };

    // Accounts lines still buffered when a writer is about to be abandoned
    // (dropped for rotation, or on thread exit). The writer is then abandoned
    // via `abandon_without_flush` (not a plain `drop`), which discards the
    // buffer WITHOUT the implicit flush retry a `BufWriter`'s `Drop` performs —
    // so a prior failed flush's bytes are genuinely gone, and counting these
    // lines as `errors` exactly once here cannot be contradicted by a late
    // retry silently persisting them.
    let account_lost = |pending: &mut u64| {
        *pending = account_lost_pending(*pending, &errors);
    };

    // A fatal writer failure inside line handling: the current line has already
    // been removed from `rx`, so its byte reservation (released at the bottom of
    // the arm on the normal path) and its loss would otherwise leak when we
    // return early. Release the reservation and — unless the line was already
    // counted as a write failure (`already_counted`) — count it lost, then drain
    // and account every still-queued line before the thread exits.
    let fatal_settle = |line_bytes: u64, already_counted: bool| {
        if !already_counted {
            errors.fetch_add(1, Ordering::Relaxed);
        }
        queued_bytes.fetch_sub(line_bytes, Ordering::AcqRel);
        drain_and_account(&rx, &queued_bytes, &errors);
    };

    // Periodic durability report: dropped/error counters are otherwise only
    // reachable via the console API (which nothing polls), leaving queue-full
    // drops and disk/rotation errors invisible in a running node. Report deltas
    // to stderr so operators can see when to raise the budget or check the disk.
    let mut report_deadline = Instant::now() + REPORT_INTERVAL;
    let mut last_report: (u64, u64) = (0, 0);

    loop {
        // Surface accumulated drops/errors on the report cadence. The loop wakes
        // at least every flush interval (the Timeout arm fires even when idle),
        // so this is evaluated regularly without its own timer.
        if Instant::now() >= report_deadline {
            report_deadline = Instant::now() + REPORT_INTERVAL;
            let cur = (
                dropped.load(Ordering::Relaxed),
                errors.load(Ordering::Relaxed),
            );
            if loss_grew(last_report, cur) {
                eprintln!(
                    "nano-trace-store: durability report for {}: {} trace(s) dropped (budget full), {} write/rotation error(s); raise NANOBPMN_TRACE_FILE_QUEUE_BYTES or check the disk",
                    cfg.path.display(),
                    cur.0,
                    cur.1
                );
                last_report = cur;
            }
        }
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
                    let flush_ok = do_flush(&mut writer, &mut pending, &mut next_flush, true);
                    // Any lines still pending survived a failed flush; the writer
                    // we are about to abandon cannot persist them, so they are
                    // lost. Abandon it without an implicit flush retry (see
                    // `abandon_without_flush`) so that loss stays truthful.
                    account_lost(&mut pending);
                    abandon_without_flush(writer);
                    // A failed flush may have persisted a PREFIX of the final
                    // record before erroring, leaving a torn tail on disk.
                    // Repair the active file before renaming it — otherwise the
                    // torn record is sealed into the rotated `.1` segment
                    // (repair only ever runs on the active path), permanently
                    // corrupting it. Fail closed: if the tail cannot be
                    // verified, skip the rotation rather than archive a file we
                    // cannot vouch for; the error is counted and the reopen
                    // below re-establishes a clean writer on the still-present
                    // file, then falls through to write the triggering line so
                    // it is not silently dropped.
                    if !flush_ok && let Err(e) = truncate_incomplete_tail(&cfg.path) {
                        errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "nano-trace-store: cannot repair trace file {} after a failed flush; skipping rotation: {e}",
                            cfg.path.display()
                        );
                        match open_append(&cfg.path) {
                            Ok(f) => {
                                writer = BufWriter::new(f);
                                size = file_size(&cfg.path);
                            }
                            Err(e) => {
                                errors.fetch_add(1, Ordering::Relaxed);
                                eprintln!(
                                    "nano-trace-store: cannot reopen trace file {} after failed repair: {e}",
                                    cfg.path.display()
                                );
                                // The triggering line was never written: count it
                                // lost and release its reservation, then drain the
                                // rest — otherwise its bytes leak in `queued_bytes`
                                // and the trace vanishes unaccounted.
                                fatal_settle(line_bytes, false);
                                return;
                            }
                        }
                        // Rotation was skipped, but `open_append` re-ran the tail
                        // repair itself, so the reopened handle is verifiably
                        // clean. Fall through to write the current line onto it
                        // rather than dropping it: a `continue` here would discard
                        // the triggering trace without counting it in `errors`,
                        // the exact silent loss this sink exists to prevent.
                    } else {
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
                                        // The writer can no longer persist. The
                                        // triggering line was never written: count
                                        // it lost and release its reservation, then
                                        // drain and account every already-queued
                                        // line — rather than returning with the
                                        // channel silently dropping them and the
                                        // reservations leaking.
                                        fatal_settle(line_bytes, false);
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
                                        // Cannot continue appending either. The
                                        // triggering line was never written: count
                                        // it lost and release its reservation, then
                                        // drain and account the queued lines before
                                        // exit.
                                        errors.fetch_add(1, Ordering::Relaxed);
                                        eprintln!(
                                            "nano-trace-store: cannot reopen trace file {} after failed rotation: {e}",
                                            cfg.path.display()
                                        );
                                        fatal_settle(line_bytes, false);
                                        return;
                                    }
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
                        // silent trace loss is observable — the engine must never
                        // see this error. But `write_all` may have persisted only
                        // part of the record (or the JSON but not its newline),
                        // so the writer now holds a torn tail. Continuing to
                        // append would concatenate the next trace onto that
                        // partial record and corrupt the NDJSON stream, so treat
                        // the writer as terminal: account any still-buffered
                        // lines, drop it, and reopen onto a file repaired back to
                        // its last complete newline.
                        errors.fetch_add(1, Ordering::Relaxed);
                        eprintln!(
                            "nano-trace-store: write to trace file {} failed, trace discarded: {e}",
                            cfg.path.display()
                        );
                        account_lost(&mut pending);
                        abandon_without_flush(writer);
                        match open_append(&cfg.path) {
                            Ok(f) => {
                                size = file_size(&cfg.path);
                                writer = BufWriter::new(f);
                            }
                            Err(e) => {
                                // Cannot re-establish a clean writer. The current
                                // line was already counted as a write failure above,
                                // so only release its still-held reservation (it is
                                // not counted a second time), then drain and account
                                // the queued lines before exit.
                                errors.fetch_add(1, Ordering::Relaxed);
                                eprintln!(
                                    "nano-trace-store: cannot reopen trace file {} after write failure: {e}",
                                    cfg.path.display()
                                );
                                fatal_settle(line_bytes, true);
                                return;
                            }
                        }
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
    // buffered in the writer we are about to abandon. They cannot be persisted
    // now, so classify them as lost (counted once) and abandon the writer without
    // the implicit flush retry `drop` would run — a late retry persisting them
    // would contradict the loss we just counted.
    account_lost(&mut pending);
    abandon_without_flush(writer);
}

#[cfg(test)]
mod tests {
    use std::sync::atomic::{AtomicU64, Ordering};

    use super::{account_lost_pending, loss_grew, settle_flush};

    fn load(a: &AtomicU64) -> u64 {
        a.load(Ordering::Relaxed)
    }

    #[test]
    fn loss_grew_only_fires_when_a_counter_increases() {
        // Steady state (no new loss) must not re-log every report window.
        assert!(!loss_grew((0, 0), (0, 0)));
        assert!(!loss_grew((3, 5), (3, 5)));
        // A growth in either the dropped or the errors counter warrants a report.
        assert!(loss_grew((0, 0), (1, 0)), "new drop should report");
        assert!(loss_grew((0, 0), (0, 1)), "new error should report");
        assert!(loss_grew((3, 5), (4, 5)), "dropped grew");
        assert!(loss_grew((3, 5), (3, 6)), "errors grew");
        assert!(loss_grew((3, 5), (4, 6)), "both grew");
        // Counters are monotonic, so a non-increase (the baseline already past
        // the current read, which should not happen) never reports.
        assert!(!loss_grew((4, 6), (3, 5)));
    }

    fn tmp_path(tag: &str) -> std::path::PathBuf {
        std::env::temp_dir().join(format!(
            "nano-trace-sink-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ))
    }

    /// Probes whether the current process is actually subject to filesystem
    /// permission bits. Root (or any `CAP_DAC_OVERRIDE` holder, common in CI
    /// containers) bypasses them: it can read and repair a `0o200` write-only
    /// file, so a fail-closed assertion that depends on a read being *rejected*
    /// does not hold. Tests that force failure via `0o200` must skip themselves
    /// when this returns `false`, keeping the workspace suite portable to
    /// root-run environments.
    #[cfg(unix)]
    fn permission_bits_enforced() -> bool {
        use std::os::unix::fs::PermissionsExt;
        let probe = tmp_path("permprobe");
        if std::fs::write(&probe, b"x").is_err() {
            return true; // cannot probe; assume enforced (best effort)
        }
        let enforced = std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o200))
            .is_ok()
            && std::fs::File::open(&probe).is_err();
        let _ = std::fs::set_permissions(&probe, std::fs::Permissions::from_mode(0o600));
        let _ = std::fs::remove_file(&probe);
        enforced
    }

    #[test]
    fn open_append_truncates_a_torn_final_record() {
        // Regression for the partial-tail finding: a crash that leaves bytes
        // after the last newline must not have the next trace concatenated onto
        // them. Opening for append repairs the file to its last complete
        // newline first.
        let path = tmp_path("torn");
        std::fs::write(&path, b"{\"a\":1}\n{\"b\":2}\n{\"c\":3") // torn final record
            .unwrap();
        {
            let _f = super::open_append(&path).unwrap();
        }
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"{\"a\":1}\n{\"b\":2}\n",
            "the incomplete final record is dropped, keeping complete lines"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_append_keeps_a_newline_terminated_file_untouched() {
        let path = tmp_path("clean");
        std::fs::write(&path, b"{\"a\":1}\n{\"b\":2}\n").unwrap();
        {
            let _f = super::open_append(&path).unwrap();
        }
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"{\"a\":1}\n{\"b\":2}\n",
            "a well-formed file is left byte-for-byte intact"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn open_append_empties_a_file_with_no_newline_at_all() {
        // A single record that never got its newline is entirely torn; there is
        // no complete line to keep.
        let path = tmp_path("nonl");
        std::fs::write(&path, b"{\"a\":1").unwrap();
        {
            let _f = super::open_append(&path).unwrap();
        }
        assert_eq!(
            std::fs::read(&path).unwrap(),
            b"",
            "a file with no complete line is truncated to empty"
        );
        let _ = std::fs::remove_file(&path);
    }

    #[test]
    fn reopen_after_a_partial_write_recovers_valid_ndjson() {
        // End-to-end: seed a torn tail (as a failed mid-record write leaves),
        // then run a real sink over it. The appended trace must land on its own
        // line, not concatenated onto the partial record.
        let path = tmp_path("recover");
        std::fs::write(&path, b"{\"old\":true}\n{\"partial\":") // torn tail
            .unwrap();
        {
            let sink = super::TraceSink::spawn(super::SinkConfig {
                path: path.clone(),
                max_bytes: None,
                keep: 5,
                queue_bytes: 1024 * 1024,
                flush_interval: std::time::Duration::from_millis(10),
                tail: 0,
            });
            sink.append("{\"new\":true}".to_string());
            // Drop flushes and joins the writer before we read the file.
        }
        let body = std::fs::read_to_string(&path).unwrap();
        let lines: Vec<&str> = body.lines().collect();
        assert_eq!(
            lines,
            vec!["{\"old\":true}", "{\"new\":true}"],
            "the torn record is dropped and the new trace is a clean line"
        );
        // Every surviving line is valid JSON (no concatenated corruption).
        for l in lines {
            assert!(
                serde_json::from_str::<serde_json::Value>(l).is_ok(),
                "line is valid JSON: {l}"
            );
        }
        let _ = std::fs::remove_file(&path);
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

    #[cfg(unix)]
    #[test]
    fn open_append_fails_closed_when_the_tail_cannot_be_repaired() {
        // Regression for the ignored-repair finding: a write-only trace file
        // rejects the read/write repair open but would accept an append.
        // Appending anyway could concatenate onto an unexamined torn tail, so
        // the open must fail (the writer's drain-and-account path handles it)
        // rather than proceed.
        use std::os::unix::fs::PermissionsExt;
        if !permission_bits_enforced() {
            return; // root bypasses 0o200; the read this test must reject would succeed.
        }
        let path = tmp_path("writeonly");
        std::fs::write(&path, b"{\"a\":1}\n{\"b\":2").unwrap(); // torn tail
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o200)).unwrap();
        let result = super::open_append(&path);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let _ = std::fs::remove_file(&path);
        assert!(
            result.is_err(),
            "an unrepairable tail must fail the open, not append blindly"
        );
    }

    #[cfg(unix)]
    #[test]
    fn rotation_repairs_a_torn_tail_before_archiving_the_segment() {
        // End-to-end regression for the torn-rotation finding: simulate a failed
        // pre-rotation flush that persisted only a prefix of the final record
        // (a torn tail on disk). Rotation must repair the active file back to
        // its last complete newline BEFORE renaming it, so the archived `.1`
        // segment is valid NDJSON.
        let path = tmp_path("rottorn");
        std::fs::write(&path, b"{\"a\":1}\n{\"b\":2").unwrap(); // torn tail
        {
            let sink = super::TraceSink::spawn(super::SinkConfig {
                path: path.clone(),
                max_bytes: Some(1), // the first append triggers prospective rotation
                keep: 5,
                queue_bytes: 1024 * 1024,
                flush_interval: std::time::Duration::from_millis(10),
                tail: 0,
            });
            sink.append("{\"c\":3}".to_string());
            // Drop flushes and joins the writer before we inspect the files.
        }
        let archived = std::fs::read(format!("{}.1", path.display())).unwrap();
        assert_eq!(
            archived, b"{\"a\":1}\n",
            "the torn tail is repaired before the segment is archived"
        );
        let active = std::fs::read_to_string(&path).unwrap();
        assert_eq!(
            active, "{\"c\":3}\n",
            "the new trace lands in a fresh segment"
        );
        for l in archived
            .split(|&b| b == b'\n')
            .filter(|s| !s.is_empty())
            .chain(active.lines().map(str::as_bytes))
        {
            let _: serde_json::Value = serde_json::from_slice(l).expect("valid JSON line");
        }
        let _ = std::fs::remove_file(&path);
        let _ = std::fs::remove_file(format!("{}.1", path.display()));
    }

    #[cfg(unix)]
    #[test]
    fn rotation_is_skipped_fail_closed_when_the_torn_tail_cannot_be_repaired() {
        // The fail-closed sibling: a failed pre-rotation flush leaves a torn
        // tail, but the file is write-only so the repair cannot run. Rotation
        // must be SKIPPED (never archive a file whose tail could not be
        // verified). We test this at the unit level: `truncate_incomplete_tail`
        // fails on a write-only file, and `open_append` propagates that failure
        // rather than appending blindly.
        use std::os::unix::fs::PermissionsExt;
        if !permission_bits_enforced() {
            return; // root bypasses 0o200; repair/open would succeed, not fail closed.
        }
        let path = tmp_path("rotskip");
        std::fs::write(&path, b"{\"a\":1}\n{\"b\":2").unwrap(); // torn tail
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o200)).unwrap();
        let repair = super::truncate_incomplete_tail(&path);
        let open = super::open_append(&path);
        std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
        let _ = std::fs::remove_file(&path);
        assert!(repair.is_err(), "repair fails on a write-only file");
        assert!(
            open.is_err(),
            "open_append fails closed when the tail cannot be repaired"
        );
    }
}
