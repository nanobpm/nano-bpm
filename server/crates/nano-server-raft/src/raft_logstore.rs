//! A crash-durable Raft log store (openraft v2 [`RaftLogStorage`]) — milestone B
//! of stage 3.
//!
//! Milestone A used an in-memory log ([`MemLogStore`](crate::raft::MemLogStore)),
//! so a client-acked command was only as durable as the applied engine state. By
//! the Raft model, the **log is the source of truth**: once an entry is fsynced
//! here and committed, a restart replays the durable log back into a fresh
//! (volatile) state machine to reconstruct engine state. That makes this store
//! the real durability boundary — the engine [`Journal`](crate::journal::Journal)
//! used by the state machine can stay in-memory, because nothing acked is lost as
//! long as it is in *this* log.
//!
//! # On-disk layout (one directory per partition replica)
//!
//! - `seg-<start>.ndjson` — the log entries, one serialized [`Entry`] per line, in
//!   index order, split into segments that roll at a byte threshold. Appended to
//!   (with `fsync`) on [`append`](RaftLogStorage::append). On
//!   [`purge`](RaftLogStorage::purge) whole sealed segments whose every entry is
//!   below the purge point are **unlinked** — never rewritten — so compaction is
//!   `O(1)` file deletes off the raft core's critical path rather than a full-log
//!   rewrite. [`truncate`](RaftLogStorage::truncate) (rare, log-conflict only)
//!   rewrites at most the single straddled segment.
//! - `vote.json` — the persisted [`Vote`]. Rewritten atomically (temp + rename +
//!   dir-fsync) on every [`save_vote`](RaftLogStorage::save_vote), because Raft
//!   correctness requires a vote to be on disk before it is acted on.
//! - `state.json` — the `last_purged` and `committed` markers, rewritten
//!   atomically when either changes. The `last_purged` marker is the durable
//!   record of a purge; physical segment deletion is best-effort and reconciled on
//!   restart (entries at or below `last_purged` are filtered out on replay), so a
//!   crash mid-purge never resurrects purged entries.
//!
//! # Durability vs. concurrency
//!
//! Writes `fsync` synchronously inside the async method (the same idiom as
//! openraft's own file/rocks example stores). openraft batches a replication
//! round's entries into a single `append`, so this is one `fsync` per round.
//! A future optimization could offload the `fsync` to a dedicated writer thread
//! (mirroring [`Journal`]'s group-commit writer) to decouple it from the raft
//! core task.

// Additive until leader routing mounts it; see `raft.rs` for the rationale.
#![allow(dead_code)]

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, Read, Seek, SeekFrom, Write};
use std::ops::RangeBounds;
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, AtomicI64, Ordering};
use std::sync::{Arc, Mutex, Weak};
use std::time::Duration;

use openraft::storage::{LogFlushed, LogState, RaftLogReader, RaftLogStorage};
use openraft::{Entry, LogId, OptionalSend, StorageError, StorageIOError, Vote};
use serde::{Deserialize, Serialize};

use crate::raft::{NodeId, RaftConfig};

/// Durability mode for the Raft log store, mirroring the engine journal
/// ([`crate::journal`]) so a node honours one `NANOBPMN_DURABILITY` setting for
/// both its applied-state journal and its replicated Raft log.
///
/// - `Sync` (default): every `append` is `fsync`ed before its openraft flush
///   callback fires, and the committed/purge markers are `fsync`ed on write. The
///   strongest contract, at the cost of a media barrier (an `F_FULLFSYNC` on
///   macOS) on every replication round.
/// - `Async`: an `append` is acknowledged once it reaches the OS page cache, and
///   `fsync` is amortised onto a background cadence (every [`AsyncFlush::interval`]
///   or [`AsyncFlush::max_bytes`]). The committed marker — an *optional* openraft
///   optimisation, re-derived from the log on restart — is likewise deferred.
///   A **process** crash loses nothing (the appended bytes survive in the page
///   cache and replay on restart); only an **OS crash / power loss** can lose the
///   unfsynced tail, bounded by the flush interval. The leader's election vote is
///   still `fsync`ed synchronously in both modes, because Raft safety requires a
///   vote to be on disk before it is acted on.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
enum DurabilityMode {
    Sync,
    Async,
}

/// Async-durability flush policy: `fsync` fires when either the bytes appended
/// since the last fsync reach `max_bytes`, or `interval` elapses (whichever is
/// first). Reuses the journal's `NANOBPMN_ASYNC_FLUSH_MS` / `_BYTES` knobs.
#[derive(Clone, Copy)]
struct AsyncFlush {
    interval: Duration,
    max_bytes: usize,
}

/// Durability mode from `NANOBPMN_DURABILITY` (`sync` | `async`). Defaults to
/// `sync` so the strong fsync-before-ack contract is unchanged unless async is
/// explicitly opted into (same default and variable as the journal).
fn durability_mode_from_env() -> DurabilityMode {
    match std::env::var("NANOBPMN_DURABILITY") {
        Ok(v) if v.trim().eq_ignore_ascii_case("async") => DurabilityMode::Async,
        _ => DurabilityMode::Sync,
    }
}

/// Process-wide "recovery fsync relief" flag. When a node becomes a failover
/// incumbent (leads partitions it does not statically own, because a peer is
/// down) or a returning owner catching its partitions back up, the surviving
/// node carries roughly double its steady Raft load on a single shared disk.
/// Under `sync` durability that doubles the per-replication-round `fsync` rate
/// and can saturate the disk, inflating `fsync` latency an order of magnitude
/// and making commits — and therefore completions and the completion-paced
/// admission servo — bursty and oscillatory.
///
/// While this flag is set, `sync`-mode stores coalesce the *log-append* and
/// *committed-marker* `fsync` onto the same bounded background cadence the
/// `async` mode already uses (at most [`AsyncFlush::interval`] / `max_bytes`
/// unfsynced) — group-committing many rounds into one media barrier. The
/// election **vote is still fsynced synchronously** (see [`save_vote`]), so the
/// Raft safety invariant is untouched; only the *optional* committed marker and
/// the page-cache-resident log tail are deferred, exactly as `async` mode. The
/// unfsynced window is bounded to the flush interval (~10 ms) and only widens
/// during the recovery window on the node already carrying the extra load, so
/// the marginal durability exposure is small and self-limiting.
///
/// The relief is a milder, measurable first step toward a full async-during-
/// recovery mode: it reuses the shipped `async` code path rather than adding a
/// new one, and reverts to strict per-round `fsync` the moment recovery clears.
static RECOVERY_FSYNC_RELIEF: AtomicBool = AtomicBool::new(false);

/// Enables/disables the recovery fsync-relief window (driven by the server's
/// recovery supervisor from the live per-partition leadership picture).
pub fn set_recovery_fsync_relief(on: bool) {
    RECOVERY_FSYNC_RELIEF.store(on, Ordering::Relaxed);
    crate::metrics::set_raft_fsync_relief(on);
}

/// Whether the recovery fsync-relief window is currently active.
pub fn recovery_fsync_relief() -> bool {
    RECOVERY_FSYNC_RELIEF.load(Ordering::Relaxed)
}

/// Whether the recovery fsync-relief fallback is enabled for this process.
/// `NANOBPMN_RECOVERY_FSYNC_COALESCE` — **default OFF**. This deliberately
/// relaxes durability during recovery (a `sync` store's committed marker + log
/// tail are deferred to the async cadence while relief is engaged; see
/// [`RECOVERY_FSYNC_RELIEF`]), so it is opt-in: the durability-preserving
/// primary lever is the adaptive recovery admission throttle (which paces intake
/// so the disk stays under its fsync knee without deferring any fsync). Leave
/// this off unless the throttle alone cannot hold the disk and you accept the
/// bounded recovery-window durability exposure. When off, a `sync` store is
/// byte-identical to the pre-feature behaviour (no background flusher, every
/// append fsynced inline) regardless of the relief flag.
fn recovery_coalesce_enabled() -> bool {
    matches!(
        std::env::var("NANOBPMN_RECOVERY_FSYNC_COALESCE"),
        Ok(ref v) if v.trim() == "1" || v.trim().eq_ignore_ascii_case("true")
    )
}

/// Whether a `sync` store defers its *committed* marker to the background flusher
/// instead of fsyncing `state.json` (an atomic write + dir-fsync) on every commit
/// advance. `NANOBPMN_RAFT_DEFER_COMMITTED` — **default OFF** (byte-identical to the
/// prior behaviour).
///
/// Unlike recovery coalescing, this does **not** relax log durability: appends are
/// still fsynced inline before their openraft flush callback fires (the append path
/// keys off [`Inner::async_durability`], which this flag does not affect), and the
/// election vote and the safety-critical `last_purged` marker are still persisted
/// synchronously. Only the committed marker — an *optional* openraft optimisation
/// that is re-derived from the durable log + membership on restart — is amortised
/// onto the async cadence. It removes one `state.json` fsync + one directory fsync
/// per commit round, which on a fsync-latency-bound, contended disk is a large
/// share of the total media barriers (the log-append fsync that gates client acks
/// then completes faster because the disk queue is no longer saturated by the
/// per-commit marker rewrites).
fn defer_committed_marker_enabled() -> bool {
    matches!(
        std::env::var("NANOBPMN_RAFT_DEFER_COMMITTED"),
        Ok(ref v) if v.trim() == "1" || v.trim().eq_ignore_ascii_case("true")
    )
}

/// Async flush policy from env. `NANOBPMN_ASYNC_FLUSH_MS` (default 10, clamped to
/// 1s) bounds the unfsynced time window; `NANOBPMN_ASYNC_FLUSH_BYTES` (default
/// 8 MiB) bounds the unfsynced byte window. Shared with the journal.
fn async_flush_from_env() -> AsyncFlush {
    let interval = match std::env::var("NANOBPMN_ASYNC_FLUSH_MS") {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(ms) => Duration::from_millis(ms.clamp(1, 1000)),
            Err(_) => Duration::from_millis(10),
        },
        Err(_) => Duration::from_millis(10),
    };
    let max_bytes = match std::env::var("NANOBPMN_ASYNC_FLUSH_BYTES") {
        Ok(v) => v.trim().parse::<usize>().unwrap_or(8 << 20).max(4096),
        Err(_) => 8 << 20,
    };
    AsyncFlush {
        interval,
        max_bytes,
    }
}

/// The durable markers persisted in `state.json`.
#[derive(Default, Serialize, Deserialize)]
struct PersistedState {
    last_purged: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
}

/// A log entry held in the in-memory index, tagged with its serialized on-disk
/// byte length and the location (segment + byte offset) of its one line in that
/// segment file.
///
/// # Byte-bounded hot-window cache
///
/// The full serialized entry is *already* durably on disk in its segment file, so
/// keeping it in RAM as well is a redundant copy. Under large (e.g. 50 KB)
/// payloads the retained post-snapshot tail (`NANOBPMN_RAFT_KEEP_LOGS` entries per
/// partition) is entry-count-bounded, so that redundant RAM copy dominates RSS.
///
/// `entry` is therefore an *optional* in-RAM cache of the on-disk line:
/// - `Some(entry)` — resident ("hot"): served directly, no I/O.
/// - `None` — demoted ("cold"): the entry lives only in `seg-<seg_start>.ndjson`
///   at `[offset, offset+len)` and is read back on demand in
///   [`try_get_log_entries`](RaftLogReader::try_get_log_entries).
///
/// The most-recent entries (which drive steady-state replication) stay resident;
/// only the colder low-index tail is demoted, keeping resident RAM under a byte
/// budget (`NANOBPMN_RAFT_LOG_RAM_BYTES`) regardless of payload size. Demotion is
/// an O(1) in-memory drop (the bytes are already on disk) off the fsync critical
/// path, so the write path is unchanged.
struct Stored {
    /// The entry's log id (kept even when demoted, so `get_log_state` and the
    /// truncate/purge bookkeeping never have to touch disk).
    log_id: LogId<NodeId>,
    /// Serialized byte length of this entry's on-disk line (the physical record:
    /// the compressed frame when log compression is on, else `JSON + '\n'`). Drives
    /// segment offset/roll bookkeeping and the on-disk `log_bytes` accounting.
    len: usize,
    /// Uncompressed in-RAM footprint of this entry (plaintext `JSON + '\n'` length),
    /// used for the resident-RAM budget and the `nanobpm_raft_log_ram_bytes` metric.
    /// Equal to `len` when compression is off; larger than `len` (by the compression
    /// ratio) when on — because the resident copy in `entry` is always uncompressed.
    ram_len: usize,
    /// Start index of the segment file holding this entry's line.
    seg_start: u64,
    /// Byte offset of this entry's line within that segment file.
    offset: usize,
    /// In-RAM copy of the entry, or `None` when demoted to descriptor-only.
    entry: Option<Entry<RaftConfig>>,
}

/// One on-disk log segment (`seg-<start>.ndjson`). Entries are appended in index
/// order to the *active* (last) segment; older segments are sealed and, once every
/// entry they hold has been purged, unlinked wholesale — no segment is ever
/// rewritten by `purge`, so log compaction is `O(1)` file deletes off the raft
/// core's critical path rather than a full-log rewrite.
struct SegMeta {
    /// The index this segment starts at (also encoded in its file name).
    start: u64,
    /// The highest index appended to this segment, or `None` while it is empty.
    last: Option<u64>,
}

/// Default active-segment roll threshold: seal the active segment and start a new
/// one once it reaches this many bytes. Bounds both the purge granule (a whole
/// sealed segment is reclaimed at once) and the worst-case `truncate` rewrite (only
/// the single straddled segment is rewritten). Overridable via `NANOBPMN_RAFT_SEG_BYTES`.
const DEFAULT_SEG_MAX_BYTES: usize = 64 << 20;

/// Active-segment roll threshold from `NANOBPMN_RAFT_SEG_BYTES` (bytes), clamped to
/// a 1 MiB floor. Read once at open.
fn seg_max_bytes_from_env() -> usize {
    std::env::var("NANOBPMN_RAFT_SEG_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .map(|n| n.max(1 << 20))
        .unwrap_or(DEFAULT_SEG_MAX_BYTES)
}

/// Default RAM budget for resident (hot) Raft log entries per partition replica:
/// 64 MiB. Entries beyond this — the colder, lower-index tail — are demoted to
/// descriptor-only and read back from their segment file on demand, so the
/// resident footprint of the retained tail stays bounded even under large
/// payloads. `0` disables demotion (every retained entry stays resident, the
/// pre-adaptation behaviour).
const DEFAULT_RAM_BUDGET_BYTES: usize = 64 << 20;

/// Resident-entry RAM budget (bytes) from `NANOBPMN_RAFT_LOG_RAM_BYTES`. `0`
/// disables demotion; any other value is used verbatim (a single oversized entry
/// can still exceed it, since at least the most-recent entry is always resident).
/// Read once at open.
fn ram_budget_from_env() -> usize {
    std::env::var("NANOBPMN_RAFT_LOG_RAM_BYTES")
        .ok()
        .and_then(|v| v.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_RAM_BUDGET_BYTES)
}

fn seg_path(dir: &Path, start: u64) -> PathBuf {
    // Zero-padded so the file names also sort lexically in index order.
    dir.join(format!("seg-{start:020}.ndjson"))
}

/// Opt-in per-entry compression of the Raft log segments, controlled by
/// `NANOBPMN_RAFT_LOG_COMPRESS` (off by default). Unlike the engine-journal codec
/// (`crate::seglog`), which is a no-op when Raft is enabled (the engine journal is
/// dormant then), this compresses the *replicated Raft log* — the dominant durable
/// writer under Raft — so large replicated payloads (e.g. 50 KB process variables)
/// shrink on disk, easing the per-node write-bandwidth wall.
fn raft_log_compress_from_env() -> bool {
    matches!(
        std::env::var("NANOBPMN_RAFT_LOG_COMPRESS").as_deref(),
        Ok("1") | Ok("true") | Ok("on") | Ok("yes")
    )
}

/// Frame magic: ASCII RS (`0x1E`). A plaintext entry line is JSON, always starting
/// with `{` (`0x7B`), never `0x1E`, so one byte distinguishes a compressed frame
/// from a plaintext line. This lets a single segment freely interleave the two —
/// the compression flag can flip across a restart while a segment stays open, and
/// pre-compression segments read back unchanged.
const FRAME_MAGIC: u8 = 0x1E;
/// Frame body is raw-deflate (mirrors the Raft wire codec in `raft_net.rs`).
const CODEC_DEFLATE: u8 = 1;
/// Frame header: `magic(1) | codec(1) | raw_len(u32 LE) | comp_len(u32 LE)`.
const FRAME_HEADER_LEN: usize = 10;
/// Don't frame entries whose serialized JSON is below this — the header + deflate
/// cost isn't worth it for small entries (heartbeat / membership / config), which
/// stay verbatim so the append critical path is untouched in the small-entry regime.
const MIN_COMPRESS_BYTES: usize = 1024;

/// Frames a single entry's serialized JSON for durable append, deflating it when
/// worthwhile. Returns `None` (write the plaintext `json + '\n'` verbatim) when the
/// entry is small, deflate fails, or the framed form wouldn't actually shrink the
/// write. Best-effort: compression never risks durability.
fn frame_entry(json: &[u8]) -> Option<Vec<u8>> {
    if json.len() < MIN_COMPRESS_BYTES {
        return None;
    }
    use flate2::{Compression, write::DeflateEncoder};
    let mut enc = DeflateEncoder::new(Vec::with_capacity(json.len() / 2), Compression::fast());
    if enc.write_all(json).is_err() {
        return None;
    }
    let comp = enc.finish().ok()?;
    // The plaintext form costs `json.len() + 1` (the trailing newline); only frame
    // when the framed form is strictly smaller (i.e. header + body <= json.len()).
    if comp.len().checked_add(FRAME_HEADER_LEN)? > json.len() {
        return None;
    }
    let raw_len = u32::try_from(json.len()).ok()?;
    let comp_len = u32::try_from(comp.len()).ok()?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + comp.len());
    frame.push(FRAME_MAGIC);
    frame.push(CODEC_DEFLATE);
    frame.extend_from_slice(&raw_len.to_le_bytes());
    frame.extend_from_slice(&comp_len.to_le_bytes());
    frame.extend_from_slice(&comp);
    Some(frame)
}

/// Deserializes one on-disk record — a compressed frame (leading [`FRAME_MAGIC`]) or
/// a plaintext JSON line (optionally newline-terminated) — back into an [`Entry`].
/// Used by both the sequential recovery scan and the random-access demoted-entry
/// read, so the two never disagree on the wire format.
fn entry_from_record(rec: &[u8]) -> io::Result<Entry<RaftConfig>> {
    if rec.first() == Some(&FRAME_MAGIC) {
        if rec.len() < FRAME_HEADER_LEN {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                "truncated raft log frame header",
            ));
        }
        let comp_len = u32::from_le_bytes([rec[6], rec[7], rec[8], rec[9]]) as usize;
        let body = rec
            .get(FRAME_HEADER_LEN..FRAME_HEADER_LEN + comp_len)
            .ok_or_else(|| {
                io::Error::new(io::ErrorKind::InvalidData, "truncated raft log frame body")
            })?;
        let raw_len = u32::from_le_bytes([rec[2], rec[3], rec[4], rec[5]]) as usize;
        let mut json = Vec::with_capacity(raw_len);
        use flate2::read::DeflateDecoder;
        DeflateDecoder::new(body).read_to_end(&mut json)?;
        serde_json::from_slice(&json).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    } else {
        let end = if rec.last() == Some(&b'\n') {
            rec.len() - 1
        } else {
            rec.len()
        };
        serde_json::from_slice(&rec[..end])
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))
    }
}

/// One entry recovered from a segment file, with the physical byte span it occupies.
struct ScannedEntry {
    index: u64,
    log_id: LogId<NodeId>,
    offset: usize,
    len: usize,
    ram_len: usize,
    entry: Entry<RaftConfig>,
}

/// Walks a segment file record-by-record, tolerating a free interleave of plaintext
/// JSON lines and compressed frames (the codec flag can flip across restarts). Each
/// record is length-delimited — a frame by its header's `comp_len`, a plaintext line
/// by its terminating `\n` — so the physical `(offset, len)` spans returned stay
/// valid for random-access reads regardless of codec. Also returns the total bytes
/// walked (the physical segment size, used for append/roll accounting). Errors on a
/// malformed/torn record, matching the pre-codec line-based scan's strictness.
fn scan_segment(path: &Path) -> io::Result<(Vec<ScannedEntry>, usize)> {
    let raw = fs::read(path)?;
    let mut out = Vec::new();
    let mut i = 0usize;
    while i < raw.len() {
        let (rec_len, is_frame) = if raw[i] == FRAME_MAGIC {
            if i + FRAME_HEADER_LEN > raw.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated raft log frame header",
                ));
            }
            let comp_len =
                u32::from_le_bytes([raw[i + 6], raw[i + 7], raw[i + 8], raw[i + 9]]) as usize;
            let len = FRAME_HEADER_LEN + comp_len;
            if i + len > raw.len() {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    "truncated raft log frame body",
                ));
            }
            (len, true)
        } else {
            match raw[i..].iter().position(|&b| b == b'\n') {
                Some(p) => (p + 1, false),
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "unterminated raft log line",
                    ));
                }
            }
        };
        let rec = &raw[i..i + rec_len];
        // Skip blank plaintext lines (defensive; the writer never emits them).
        if !is_frame && rec.iter().all(|b| b.is_ascii_whitespace()) {
            i += rec_len;
            continue;
        }
        let entry = entry_from_record(rec)?;
        // `ram_len` is the uncompressed in-RAM footprint. Plaintext records are
        // already `JSON + '\n'`, so `rec_len` is exact; a compressed frame's on-disk
        // `rec_len` understates the resident copy, so recover the plaintext length by
        // re-serializing (startup-only cost). Falls back to `rec_len` if that fails.
        let ram_len = if is_frame {
            serde_json::to_vec(&entry)
                .map(|v| v.len() + 1)
                .unwrap_or(rec_len)
        } else {
            rec_len
        };
        out.push(ScannedEntry {
            index: entry.log_id.index,
            log_id: entry.log_id,
            offset: i,
            len: rec_len,
            ram_len,
            entry,
        });
        i += rec_len;
    }
    Ok((out, raw.len()))
}

/// Parses a segment file name (`seg-<start>.ndjson`) back to its start index.
fn parse_seg_start(name: &str) -> Option<u64> {
    name.strip_prefix("seg-")?
        .strip_suffix(".ndjson")?
        .parse()
        .ok()
}

struct Inner {
    dir: PathBuf,
    /// Open append handle on the *active* (last) segment. Replaced when the active
    /// segment rolls or is rewritten by `truncate`.
    active_file: File,
    /// Sealed + active segments, sorted by `start`; the last element is active.
    segments: Vec<SegMeta>,
    /// Serialized byte size of the active segment, for roll decisions.
    active_bytes: usize,
    /// Roll the active segment once `active_bytes` reaches this.
    seg_max_bytes: usize,
    /// All present (non-purged) log entries, keyed by index, for fast reads. Each
    /// carries its serialized byte length so purge/truncate can keep `log_bytes` in
    /// step without re-serializing.
    log: BTreeMap<u64, Stored>,
    /// Serialized byte footprint of the in-memory `log` (kept in step with it),
    /// published to the aggregate `nanobpm_raft_log_bytes` gauge for RSS-balloon
    /// attribution.
    log_bytes: usize,
    /// Serialized byte footprint of the entries currently *resident* in RAM (the
    /// subset of `log` whose `entry` is `Some`). Kept in step with demote/rehydrate
    /// and purge/truncate, and published to `nanobpm_raft_log_ram_bytes`. Held at
    /// or below `ram_budget` by the hot-window cache.
    ram_bytes: usize,
    /// Resident-RAM byte budget for retained log entries; `0` disables demotion.
    ram_budget: usize,
    /// Demotion frontier: every present index strictly below this has already been
    /// demoted (`entry == None`), so `enforce_ram_budget` only scans from here up.
    /// Invariant: nothing below it is ever resident (purge only drops below it;
    /// truncate only drops a recent suffix; append only adds above it).
    demote_scan_from: u64,
    last_purged: Option<LogId<NodeId>>,
    committed: Option<LogId<NodeId>>,
    vote: Option<Vote<NodeId>>,
    /// Durability mode (`sync`/`async`), fixed at open from the environment.
    mode: DurabilityMode,
    /// Async flush thresholds (unused in sync mode).
    flush: AsyncFlush,
    /// Async only: bytes appended to the active segment since the last `fsync`.
    unsynced_bytes: usize,
    /// Async only: the committed/purge markers changed in memory but `state.json`
    /// has not yet been durably rewritten. The background flusher persists it.
    state_dirty: bool,
    /// Per-entry compression of appended segment records (`NANOBPMN_RAFT_LOG_COMPRESS`).
    /// Fixed at open. Reads stay codec-agnostic (frames are self-describing), so
    /// flipping this across a restart safely interleaves plaintext and framed records.
    compress: bool,
    /// Whether this store may coalesce its `sync`-mode `fsync`s onto the async
    /// cadence while [`RECOVERY_FSYNC_RELIEF`] is set (`NANOBPMN_RECOVERY_FSYNC_COALESCE`,
    /// default on). Fixed at open; also gates spawning the background flusher for
    /// a `sync` store so the relieved tail is bounded when the partition goes quiet.
    coalesce_capable: bool,
    /// Whether a `sync` store defers its *committed* marker to the background
    /// flusher instead of fsyncing `state.json` on every commit advance
    /// (`NANOBPMN_RAFT_DEFER_COMMITTED`, default off). Fixed at open. Does not relax
    /// log durability (appends still fsync inline) — see [`defer_committed_marker_enabled`].
    defer_committed: bool,
}

impl Inner {
    /// Whether this store should acknowledge appends before `fsync` and defer the
    /// media barrier to the background flusher — i.e. behave like `async` mode.
    /// True when the base mode is `async`, or when a `sync` store is inside the
    /// recovery fsync-relief window (see [`RECOVERY_FSYNC_RELIEF`]). The vote is
    /// unaffected (always synchronous); only the log tail + committed marker are
    /// deferred, bounded by the flush cadence.
    fn async_durability(&self) -> bool {
        matches!(self.mode, DurabilityMode::Async)
            || (self.coalesce_capable && recovery_fsync_relief())
    }

    /// Demotes the coldest (lowest-index) resident entries to descriptor-only until
    /// the resident footprint is at or below `ram_budget`, keeping the most-recent
    /// entries in RAM for hot-path replication. A no-op when demotion is disabled
    /// (`ram_budget == 0`) or already within budget. Demotion just drops the in-RAM
    /// `Entry` (its bytes are already durably on disk in the segment file), so it
    /// does no I/O and never touches the fsync critical path.
    ///
    /// The single most-recent entry is always kept resident so a just-appended
    /// entry — the one steady-state replication needs — is never served from disk.
    fn enforce_ram_budget(&mut self) {
        if self.ram_budget == 0 || self.ram_bytes <= self.ram_budget {
            return;
        }
        let newest = self.log.keys().next_back().copied();
        let mut freed = 0usize;
        let mut cursor = self.demote_scan_from;
        while self.ram_bytes > self.ram_budget {
            // Find the lowest-index still-resident entry at or above the frontier.
            let next = self
                .log
                .range(cursor..)
                .find(|(_, s)| s.entry.is_some())
                .map(|(idx, _)| *idx);
            let Some(idx) = next else { break };
            // Never demote the newest entry (hot replication target).
            if Some(idx) == newest {
                break;
            }
            if let Some(s) = self.log.get_mut(&idx) {
                s.entry = None;
                self.ram_bytes -= s.ram_len;
                freed += s.ram_len;
            }
            cursor = idx + 1;
        }
        self.demote_scan_from = cursor;
        if freed > 0 {
            crate::metrics::raft_log_ram_delta(-(freed as i64));
        }
    }

    /// Rolls the active segment: seals the current one and opens a fresh
    /// `seg-<start>.ndjson` as the new active append target. Called when the active
    /// segment reaches `seg_max_bytes`, so a single segment never grows without
    /// bound and purge/truncate stay cheap.
    fn roll_active(&mut self, start: u64) -> io::Result<()> {
        let path = seg_path(&self.dir, start);
        let file = OpenOptions::new().create(true).append(true).open(&path)?;
        // A fresh empty file needs the directory entry itself to be durable.
        fsync_dir(&self.dir)?;
        self.active_file = file;
        self.active_bytes = 0;
        self.segments.push(SegMeta { start, last: None });
        Ok(())
    }

    /// Async only: `fsync` the appended tail (if any) and durably persist the
    /// committed/purge markers (if changed) since the last flush. A no-op when
    /// nothing is outstanding, so the idle-tick path is cheap.
    fn flush_async(&mut self) -> io::Result<()> {
        if self.unsynced_bytes > 0 {
            timed_log_sync(&self.active_file)?;
            self.unsynced_bytes = 0;
        }
        if self.state_dirty {
            let bytes = serde_json::to_vec(&PersistedState {
                last_purged: self.last_purged,
                committed: self.committed,
            })?;
            atomic_write(&self.dir, &state_path(&self.dir), &bytes)?;
            self.state_dirty = false;
        }
        Ok(())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // Best-effort flush of any unfsynced async/relieved tail on shutdown so a
        // clean stop leaves nothing unfsynced. A no-op for a sync store outside
        // the relief window (its appends already fsynced inline, so nothing is
        // outstanding), so this is safe to call unconditionally.
        let _ = self.flush_async();
    }
}

/// A crash-durable Raft log store. Cloning shares the same underlying log (the
/// [`RaftLogReader`] handed to openraft is a clone), so all access is serialized
/// through a single [`Mutex`].
#[derive(Clone)]
pub struct RaftLogStore {
    inner: Arc<Mutex<Inner>>,
    /// Per-partition live (non-purged) log byte footprint, republished after every
    /// append/truncate/purge. The openraft `Raft` handle erases the concrete log
    /// store type, so the compaction governor cannot read `Inner::log_bytes`
    /// directly; this shared handle is the bridge it reads to make byte-based
    /// snapshot/compaction decisions for the partition.
    bytes: Arc<AtomicI64>,
}

fn log_path(dir: &Path) -> PathBuf {
    dir.join("log.ndjson")
}
fn vote_path(dir: &Path) -> PathBuf {
    dir.join("vote.json")
}
fn state_path(dir: &Path) -> PathBuf {
    dir.join("state.json")
}

/// Cheaply peek the durable `(committed, last_purged)` markers from a partition's
/// `state.json` WITHOUT opening the whole log store (no segment replay). Used by
/// the boot purge-hole detector ([`crate::raft::durable_log_has_purge_hole`]) to
/// decide whether hosting the partition from its on-disk log would trip openraft's
/// defensive `LogIndexNotFound` (the reapply range is purged) — in which case the
/// caller hosts a fresh receiver and installs a snapshot from the leader instead.
/// Any read/parse error is treated as "no markers" (returns `(None, None)`), which
/// yields "no hole" — the conservative choice that preserves the on-disk lineage.
pub fn peek_committed_and_purged(dir: &Path) -> (Option<LogId<NodeId>>, Option<LogId<NodeId>>) {
    let state: PersistedState = read_json(&state_path(dir))
        .ok()
        .flatten()
        .unwrap_or_default();
    (state.committed, state.last_purged)
}

/// `fsync` the directory so a preceding `rename` is itself durable.
fn fsync_dir(dir: &Path) -> io::Result<()> {
    File::open(dir)?.sync_all()
}

/// `sync_all()` a Raft-log data file, timing the barrier into the
/// `nanobpm_raft_fsync_seconds` histogram. This is the disk-saturation signal the
/// recovery admission throttle reads: on a failover node carrying a down peer's
/// partitions these barriers are what saturate the shared disk. Records the
/// elapsed time even on error (the barrier still consumed disk time).
fn timed_log_sync(file: &File) -> io::Result<()> {
    let t0 = std::time::Instant::now();
    let r = file.sync_all();
    crate::metrics::observe_raft_fsync(t0.elapsed());
    r
}

/// Atomically replace `path`'s contents with `bytes`: write a sibling temp file,
/// `fsync` it, `rename` over the target, then `fsync` the directory.
fn atomic_write(dir: &Path, path: &Path, bytes: &[u8]) -> io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&tmp)?;
        f.write_all(bytes)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    fsync_dir(dir)
}

/// Migrates a pre-segmentation `log.ndjson` into a `seg-<start>.ndjson` segment so
/// an in-place binary upgrade keeps its durable log. Renames the legacy file to a
/// segment named for its first entry's index; a no-op when the file is absent, and
/// a cleanup when segments already exist alongside it.
fn migrate_legacy_log(dir: &Path) -> io::Result<()> {
    let legacy = log_path(dir);
    if !legacy.exists() {
        return Ok(());
    }
    let has_segments = fs::read_dir(dir)?
        .filter_map(|e| e.ok())
        .any(|e| e.file_name().to_str().and_then(parse_seg_start).is_some());
    if has_segments {
        // Segments are authoritative; the legacy file is stale.
        let _ = fs::remove_file(&legacy);
        return Ok(());
    }
    let mut first: Option<u64> = None;
    let reader = BufReader::new(File::open(&legacy)?);
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        let entry: Entry<RaftConfig> = serde_json::from_str(&line)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        first = Some(entry.log_id.index);
        break;
    }
    match first {
        Some(start) => {
            fs::rename(&legacy, seg_path(dir, start))?;
            fsync_dir(dir)?;
        }
        None => {
            let _ = fs::remove_file(&legacy);
        }
    }
    Ok(())
}

impl RaftLogStore {
    /// A shared handle to this partition's live log byte footprint, kept current
    /// after every append/truncate/purge. Read by the compaction governor to make
    /// byte-based snapshot decisions (the openraft `Raft` handle hides the store
    /// type, so this is the only way to observe per-partition log bytes).
    pub fn bytes_handle(&self) -> Arc<AtomicI64> {
        self.bytes.clone()
    }

    /// Test-only: whether an append would DEFER its `fsync` (coalesce onto the
    /// async cadence) rather than fsync inline right now, given the store's mode,
    /// `coalesce_capable`, and the live recovery-relief flag. Mirrors the exact
    /// predicate the `append`/`save_committed` fsync paths branch on.
    #[cfg(test)]
    fn would_defer_fsync(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .async_durability()
    }

    /// Test-only: force `coalesce_capable` on/off without touching the process
    /// environment (which would race other parallel tests that `open()` a store).
    #[cfg(test)]
    fn force_coalesce_capable(&self, on: bool) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .coalesce_capable = on;
    }

    /// Test-only: force the sync-mode committed-marker deferral on/off without
    /// touching the process environment.
    #[cfg(test)]
    fn force_defer_committed(&self, on: bool) {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .defer_committed = on;
    }

    /// Test-only: whether the committed/purge markers are dirty in memory (awaiting
    /// a background-flusher persist to `state.json`).
    #[cfg(test)]
    fn is_state_dirty(&self) -> bool {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .state_dirty
    }

    /// Test-only: run one background-flusher pass synchronously.
    #[cfg(test)]
    fn flush_now(&self) -> io::Result<()> {
        self.inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .flush_async()
    }

    /// Opens (creating if absent) a durable log store rooted at `dir`, replaying
    /// any existing `seg-*.ndjson` segments (or a legacy `log.ndjson`), `vote.json`
    /// and `state.json` to reconstruct the in-memory index, the persisted vote and
    /// the purge/commit markers.
    pub fn open(dir: impl AsRef<Path>) -> io::Result<Self> {
        let dir = dir.as_ref().to_path_buf();
        fs::create_dir_all(&dir)?;

        let vote: Option<Vote<NodeId>> = read_json(&vote_path(&dir))?;
        let state: PersistedState = read_json(&state_path(&dir))?.unwrap_or_default();
        let purged_upto = state.last_purged.map(|l| l.index);

        // Migrate a legacy single-file `log.ndjson` (pre-segmentation format) into a
        // segment named for its first entry's index, so an in-place binary upgrade
        // keeps its durable log.
        migrate_legacy_log(&dir)?;

        // Discover segment files, sorted by start index.
        let mut starts: Vec<u64> = Vec::new();
        for ent in fs::read_dir(&dir)? {
            let ent = ent?;
            if let Some(start) = ent.file_name().to_str().and_then(parse_seg_start) {
                starts.push(start);
            }
        }
        starts.sort_unstable();

        // Replay every segment into the in-memory index. Entries at or below the
        // durable `last_purged` marker are physically present (deletion is
        // best-effort) but filtered out here so a crash mid-purge never resurrects
        // purged entries. A sealed segment entirely at or below the marker is pure
        // garbage — unlink it now to reclaim the space.
        let mut log: BTreeMap<u64, Stored> = BTreeMap::new();
        let mut log_bytes: usize = 0;
        let mut ram_total: usize = 0;
        let mut segments: Vec<SegMeta> = Vec::new();
        let mut active_bytes: usize = 0;
        for (pos, &start) in starts.iter().enumerate() {
            let is_last = pos + 1 == starts.len();
            let path = seg_path(&dir, start);
            let (records, seg_bytes) = scan_segment(&path)?;
            let mut last: Option<u64> = None;
            for r in records {
                last = Some(r.index);
                if purged_upto.map(|p| r.index > p).unwrap_or(true) {
                    log_bytes += r.len;
                    ram_total += r.ram_len;
                    log.insert(
                        r.index,
                        Stored {
                            log_id: r.log_id,
                            len: r.len,
                            ram_len: r.ram_len,
                            seg_start: start,
                            offset: r.offset,
                            entry: Some(r.entry),
                        },
                    );
                }
            }
            let fully_purged = matches!((last, purged_upto), (Some(l), Some(p)) if l <= p);
            if fully_purged && !is_last {
                let _ = fs::remove_file(&path);
                continue;
            }
            if is_last {
                active_bytes = seg_bytes;
            }
            segments.push(SegMeta { start, last });
        }

        // Ensure there is always an active segment to append to.
        if segments.is_empty() {
            segments.push(SegMeta {
                start: 0,
                last: None,
            });
            active_bytes = 0;
        }
        let active_start = segments.last().map(|s| s.start).unwrap_or(0);
        let active_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(seg_path(&dir, active_start))?;

        let mode = durability_mode_from_env();
        let entries = log.len() as i64;
        let store = Self {
            inner: Arc::new(Mutex::new(Inner {
                dir,
                active_file,
                segments,
                active_bytes,
                seg_max_bytes: seg_max_bytes_from_env(),
                log,
                log_bytes,
                ram_bytes: ram_total,
                ram_budget: ram_budget_from_env(),
                demote_scan_from: 0,
                last_purged: state.last_purged,
                committed: state.committed,
                vote,
                mode,
                flush: async_flush_from_env(),
                unsynced_bytes: 0,
                state_dirty: false,
                compress: raft_log_compress_from_env(),
                coalesce_capable: recovery_coalesce_enabled(),
                defer_committed: defer_committed_marker_enabled(),
            })),
            bytes: Arc::new(AtomicI64::new(log_bytes as i64)),
        };
        crate::metrics::raft_log_delta(entries, log_bytes as i64);
        crate::metrics::raft_log_ram_delta(ram_total as i64);
        // A recovered tail loads fully resident; demote its cold prefix so a
        // restart under a large-payload workload does not re-balloon RAM.
        store
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .enforce_ram_budget();

        // The async flusher bounds the unfsynced window when the partition goes
        // quiet (an idle follower would otherwise hold an unflushed tail
        // indefinitely). It is needed in `async` mode, and also for a `sync`
        // store that may enter the recovery fsync-relief window: the ticker
        // fsyncs the coalesced tail within `flush.interval`. It holds a `Weak`,
        // so it exits once the Raft instance drops the store — no explicit
        // shutdown handshake needed. When neither applies it is not spawned, so
        // a pure `sync` store with coalescing disabled keeps its exact prior
        // thread model (no background flusher, every append fsynced inline).
        if mode == DurabilityMode::Async
            || recovery_coalesce_enabled()
            || defer_committed_marker_enabled()
        {
            store.spawn_flusher();
        }

        Ok(store)
    }

    /// Spawns the async-durability background flusher (async mode only). Wakes
    /// every `flush.interval` and `fsync`s the appended tail / persists the
    /// committed marker if anything is outstanding. Exits when the last strong
    /// reference to the store is dropped.
    fn spawn_flusher(&self) {
        let weak: Weak<Mutex<Inner>> = Arc::downgrade(&self.inner);
        let interval = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.flush.interval
        };
        std::thread::Builder::new()
            .name("nanobpmn-raft-log-flusher".into())
            .spawn(move || {
                loop {
                    std::thread::sleep(interval);
                    let Some(inner) = weak.upgrade() else {
                        break; // store dropped — stop ticking
                    };
                    let mut inner = inner.lock().unwrap_or_else(|e| e.into_inner());
                    if let Err(e) = inner.flush_async() {
                        tracing::error!(
                            "raft log flusher fsync failed: {e}; aborting to avoid \
                             serving non-durable replicated state"
                        );
                        std::process::abort();
                    }
                }
            })
            .ok();
    }

    /// Rebuilds the on-disk segments from the in-memory index after a `truncate`
    /// removed a tail of entries. Only the (at most one) segment straddling the
    /// truncation point is rewritten and any segments entirely above it are
    /// unlinked, so the cost is bounded by a single segment — not the whole log.
    #[allow(clippy::result_large_err)]
    fn rewrite_after_truncate(inner: &mut Inner, since: u64) -> Result<(), StorageError<NodeId>> {
        // Drop segments that start at or after the truncation point (fully removed).
        let mut kept: Vec<SegMeta> = Vec::new();
        for seg in std::mem::take(&mut inner.segments) {
            if seg.start >= since {
                let _ = fs::remove_file(seg_path(&inner.dir, seg.start));
            } else {
                kept.push(seg);
            }
        }
        // If nothing survives, start a fresh active segment at the truncation point.
        if kept.is_empty() {
            let path = seg_path(&inner.dir, since);
            let file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&path)
                .map_err(io_err)?;
            file.sync_all().map_err(io_err)?;
            fsync_dir(&inner.dir).map_err(io_err)?;
            inner.active_file = file;
            inner.active_bytes = 0;
            inner.unsynced_bytes = 0;
            inner.segments = vec![SegMeta {
                start: since,
                last: None,
            }];
            return Ok(());
        }
        // Rewrite the straddling (now last-kept) segment down to just its surviving
        // prefix. The kept entries are a prefix of the segment written at unchanged
        // byte offsets, so we truncate the file to the end of the last survivor
        // rather than re-serializing — which also avoids rehydrating any demoted
        // (descriptor-only) entries and keeps every `Stored.offset` valid.
        let Some(active) = kept.last() else {
            // Unreachable: the empty case returned above with a fresh segment.
            // Guard anyway so a refactor can never turn this into a panic.
            return Err(io_err(std::io::Error::new(
                std::io::ErrorKind::InvalidData,
                "rewrite_after_truncate: no surviving segment",
            )));
        };
        let active_start = active.start;
        let path = seg_path(&inner.dir, active_start);
        let mut cut = 0usize;
        let mut last: Option<u64> = None;
        for (idx, stored) in inner.log.range(active_start..) {
            cut = stored.offset + stored.len;
            last = Some(*idx);
        }
        let existing = fs::read(&path).map_err(io_err)?;
        let cut = cut.min(existing.len());
        atomic_write(&inner.dir, &path, &existing[..cut]).map_err(io_err)?;
        let active_file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&path)
            .map_err(io_err)?;
        inner.active_file = active_file;
        inner.active_bytes = cut;
        inner.unsynced_bytes = 0;
        let n = kept.len();
        kept[n - 1].last = last;
        inner.segments = kept;
        Ok(())
    }

    #[allow(clippy::result_large_err)]
    fn persist_state(inner: &Inner) -> Result<(), StorageError<NodeId>> {
        let state = PersistedState {
            last_purged: inner.last_purged,
            committed: inner.committed,
        };
        let bytes = serde_json::to_vec(&state).map_err(io_err)?;
        atomic_write(&inner.dir, &state_path(&inner.dir), &bytes).map_err(io_err)
    }
}

/// Reads back a single demoted log entry from its on-disk segment: seeks to the
/// recorded `offset` in `seg-<seg_start>.ndjson`, reads exactly `len` bytes (the
/// physical record — a plaintext JSON line with its trailing `\n`, or a compressed
/// frame), and deserializes it. The bytes were written by `append`; on the same
/// process they are visible via the page cache even before an fsync, so a demoted
/// entry is always readable while live.
fn read_entry_at(
    dir: &Path,
    seg_start: u64,
    offset: usize,
    len: usize,
) -> io::Result<Entry<RaftConfig>> {
    let path = seg_path(dir, seg_start);
    let mut f = File::open(&path)?;
    f.seek(SeekFrom::Start(offset as u64))?;
    let mut buf = vec![0u8; len];
    f.read_exact(&mut buf)?;
    // The record is either a plaintext JSON line (with trailing newline) or a
    // self-describing compressed frame; `entry_from_record` handles both.
    entry_from_record(&buf)
}

/// Reads and deserializes a JSON file, returning `None` if it does not exist.
fn read_json<T: for<'de> Deserialize<'de>>(path: &Path) -> io::Result<Option<T>> {
    match fs::read(path) {
        Ok(bytes) if bytes.is_empty() => Ok(None),
        Ok(bytes) => serde_json::from_slice(&bytes)
            .map(Some)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e)),
        Err(e) if e.kind() == io::ErrorKind::NotFound => Ok(None),
        Err(e) => Err(e),
    }
}

/// Maps any `io`/serde error into an openraft read/write storage error.
fn io_err<E: std::error::Error + 'static>(e: E) -> StorageError<NodeId> {
    StorageIOError::write(&e).into()
}

impl RaftLogReader<RaftConfig> for RaftLogStore {
    async fn try_get_log_entries<RB: RangeBounds<u64> + Clone + Debug + OptionalSend>(
        &mut self,
        range: RB,
    ) -> Result<Vec<Entry<RaftConfig>>, StorageError<NodeId>> {
        // Snapshot the range under the lock into either a resident clone or a
        // (segment, offset, len) descriptor, then release the lock before doing any
        // disk reads so demoted-entry rehydration never serialises other appends
        // behind the store mutex.
        enum Resolved {
            Ram(Entry<RaftConfig>),
            Disk {
                seg_start: u64,
                offset: usize,
                len: usize,
            },
        }
        let planned: Vec<Resolved> = {
            let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
            // Guard against a degenerate (inverted/empty) range before touching
            // the BTreeMap, which would otherwise panic and take the node down.
            // See `crate::raft::clamp_log_range`.
            let Some(bounds) = crate::raft::clamp_log_range(&range) else {
                return Ok(Vec::new());
            };
            inner
                .log
                .range(bounds)
                .map(|(_, s)| match &s.entry {
                    Some(e) => Resolved::Ram(e.clone()),
                    None => Resolved::Disk {
                        seg_start: s.seg_start,
                        offset: s.offset,
                        len: s.len,
                    },
                })
                .collect()
        };
        let dir = {
            self.inner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .dir
                .clone()
        };
        let mut out = Vec::with_capacity(planned.len());
        for r in planned {
            match r {
                Resolved::Ram(e) => out.push(e),
                Resolved::Disk {
                    seg_start,
                    offset,
                    len,
                } => out.push(read_entry_at(&dir, seg_start, offset, len).map_err(io_err)?),
            }
        }
        Ok(out)
    }
}

impl RaftLogStorage<RaftConfig> for RaftLogStore {
    type LogReader = Self;

    async fn get_log_state(&mut self) -> Result<LogState<RaftConfig>, StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let last_log_id = inner
            .log
            .values()
            .next_back()
            .map(|s| s.log_id)
            .or(inner.last_purged);
        Ok(LogState {
            last_purged_log_id: inner.last_purged,
            last_log_id,
        })
    }

    async fn get_log_reader(&mut self) -> Self::LogReader {
        self.clone()
    }

    async fn save_vote(&mut self, vote: &Vote<NodeId>) -> Result<(), StorageError<NodeId>> {
        let inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        // The vote must hit disk before openraft acts on it.
        let bytes = serde_json::to_vec(vote).map_err(io_err)?;
        atomic_write(&inner.dir, &vote_path(&inner.dir), &bytes).map_err(io_err)?;
        drop(inner);
        self.inner.lock().unwrap_or_else(|e| e.into_inner()).vote = Some(*vote);
        Ok(())
    }

    async fn read_vote(&mut self) -> Result<Option<Vote<NodeId>>, StorageError<NodeId>> {
        Ok(self.inner.lock().unwrap_or_else(|e| e.into_inner()).vote)
    }

    async fn append<I>(
        &mut self,
        entries: I,
        callback: LogFlushed<RaftConfig>,
    ) -> Result<(), StorageError<NodeId>>
    where
        I: IntoIterator<Item = Entry<RaftConfig>> + OptionalSend,
        I::IntoIter: OptionalSend,
    {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let mut added = 0i64;
        let mut new_bytes = 0usize;
        let mut new_ram = 0usize;
        for entry in entries {
            let mut json = Vec::new();
            serde_json::to_writer(&mut json, &entry).map_err(io_err)?;
            // Uncompressed in-RAM footprint (plaintext `JSON + '\n'`), captured before
            // the compress branch may reframe/extend `json`. This — not the possibly
            // compressed on-disk `len` — is what the resident `entry` copy actually
            // costs, so it drives the RAM budget/metric.
            let ram_len = json.len() + 1;
            // Compress large entries into a self-describing frame when enabled; small
            // entries (and the disabled path) stay verbatim as `json + '\n'`. `len` is
            // the physical on-disk record length either way, so the offset/roll/demote
            // bookkeeping below is codec-agnostic.
            let line = if inner.compress {
                frame_entry(&json).unwrap_or_else(|| {
                    json.push(b'\n');
                    json
                })
            } else {
                json.push(b'\n');
                json
            };
            let len = line.len();
            let index = entry.log_id.index;
            let log_id = entry.log_id;

            // Roll to a fresh segment once the active one is full (and non-empty, so
            // a single oversized entry still lands somewhere). The outgoing segment
            // is fsynced before the switch so a sealed segment is always durable
            // regardless of durability mode.
            if inner.active_bytes > 0 && inner.active_bytes + len > inner.seg_max_bytes {
                timed_log_sync(&inner.active_file).map_err(io_err)?;
                inner.unsynced_bytes = 0;
                inner.roll_active(index).map_err(io_err)?;
            }

            // Record where this entry's line lands (post-roll) so it can later be
            // demoted to descriptor-only and read back from the segment file.
            let seg_start = inner.segments.last().map(|s| s.start).unwrap_or(index);
            let offset = inner.active_bytes;
            inner.active_file.write_all(&line).map_err(io_err)?;
            inner.active_bytes += len;
            if let Some(seg) = inner.segments.last_mut() {
                seg.last = Some(index);
            }
            inner.log.insert(
                index,
                Stored {
                    log_id,
                    len,
                    ram_len,
                    seg_start,
                    offset,
                    entry: Some(entry),
                },
            );
            inner.log_bytes += len;
            inner.ram_bytes += ram_len;
            inner.unsynced_bytes += len;
            new_bytes += len;
            new_ram += ram_len;
            added += 1;
            crate::metrics::raft_log_entry_appended(len);
        }
        match inner.async_durability() {
            // Sync: fsync before acknowledging — a flushed entry is power-loss
            // durable. The media barrier (an `F_FULLFSYNC` on macOS) is on the
            // critical path of every replication round.
            false => {
                timed_log_sync(&inner.active_file).map_err(io_err)?;
                inner.unsynced_bytes = 0;
            }
            // Async (base mode, or a sync store inside the recovery fsync-relief
            // window): the bytes are in the page cache (process-crash durable);
            // acknowledge now and let the background flusher amortise the fsync.
            // A byte-bounded inline flush caps the unfsynced window under a flood,
            // when the periodic tick alone could fall behind.
            true => {
                if inner.unsynced_bytes >= inner.flush.max_bytes {
                    timed_log_sync(&inner.active_file).map_err(io_err)?;
                    inner.unsynced_bytes = 0;
                }
            }
        }
        crate::metrics::raft_log_delta(added, new_bytes as i64);
        crate::metrics::raft_log_ram_delta(new_ram as i64);
        // Demote the cold tail back under the RAM budget (O(1) in-memory drops; the
        // bytes are already durably on disk). Off the fsync path above.
        inner.enforce_ram_budget();
        self.bytes.store(inner.log_bytes as i64, Ordering::Relaxed);
        drop(inner);
        callback.log_io_completed(Ok(()));
        Ok(())
    }

    async fn truncate(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Remove everything from `log_id.index` onward (inclusive), then rewrite at
        // most the single straddled segment (log conflicts are rare and bounded).
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let before_entries = inner.log.len() as i64;
        let before_bytes = inner.log_bytes as i64;
        let removed = inner.log.split_off(&log_id.index);
        let removed_bytes: usize = removed.values().map(|s| s.len).sum();
        let removed_ram: usize = removed
            .values()
            .filter(|s| s.entry.is_some())
            .map(|s| s.ram_len)
            .sum();
        inner.log_bytes = inner.log_bytes.saturating_sub(removed_bytes);
        inner.ram_bytes = inner.ram_bytes.saturating_sub(removed_ram);
        Self::rewrite_after_truncate(&mut inner, log_id.index)?;
        crate::metrics::raft_log_delta(
            inner.log.len() as i64 - before_entries,
            inner.log_bytes as i64 - before_bytes,
        );
        crate::metrics::raft_log_ram_delta(-(removed_ram as i64));
        self.bytes.store(inner.log_bytes as i64, Ordering::Relaxed);
        Ok(())
    }

    async fn purge(&mut self, log_id: LogId<NodeId>) -> Result<(), StorageError<NodeId>> {
        // Drop everything up to and including `log_id.index`, keep the rest. This is
        // deliberately cheap: split the in-memory index, reclaim whole sealed
        // segments by unlinking them (never a rewrite), and persist the small purge
        // marker. It runs inline on openraft's core task, so it must not do
        // unbounded work — the durable record of the purge is the `state.json`
        // marker; physical segment deletion is best-effort and reconciled on
        // restart, so a failed unlink or a crash mid-purge never resurrects entries.
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        let before_entries = inner.log.len() as i64;
        let before_bytes = inner.log_bytes as i64;
        inner.last_purged = Some(log_id);
        let keep = inner.log.split_off(&(log_id.index + 1));
        let dropped = std::mem::replace(&mut inner.log, keep);
        let dropped_bytes: usize = dropped.values().map(|s| s.len).sum();
        let dropped_ram: usize = dropped
            .values()
            .filter(|s| s.entry.is_some())
            .map(|s| s.ram_len)
            .sum();
        inner.log_bytes = inner.log_bytes.saturating_sub(dropped_bytes);
        inner.ram_bytes = inner.ram_bytes.saturating_sub(dropped_ram);
        crate::metrics::raft_log_ram_delta(-(dropped_ram as i64));

        // Reclaim whole sealed segments now entirely below the purge point.
        let upto = log_id.index;
        let dir = inner.dir.clone();
        let seg_count = inner.segments.len();
        let mut kept: Vec<SegMeta> = Vec::with_capacity(seg_count);
        for (i, seg) in std::mem::take(&mut inner.segments).into_iter().enumerate() {
            let is_active = i + 1 == seg_count;
            let fully_purged = seg.last.map(|l| l <= upto).unwrap_or(false);
            if fully_purged && !is_active {
                let _ = fs::remove_file(seg_path(&dir, seg.start));
            } else {
                kept.push(seg);
            }
        }
        inner.segments = kept;

        crate::metrics::raft_log_delta(
            inner.log.len() as i64 - before_entries,
            inner.log_bytes as i64 - before_bytes,
        );
        self.bytes.store(inner.log_bytes as i64, Ordering::Relaxed);
        // Persist the durable purge marker (a small atomic write whose directory
        // fsync also makes the segment unlinks durable). No log rewrite.
        inner.state_dirty = false;
        Self::persist_state(&inner)
    }

    async fn save_committed(
        &mut self,
        committed: Option<LogId<NodeId>>,
    ) -> Result<(), StorageError<NodeId>> {
        let mut inner = self.inner.lock().unwrap_or_else(|e| e.into_inner());
        inner.committed = committed;
        // Defer the committed marker to the background flusher when either the store
        // is behaving as `async` (base async mode or the recovery fsync-relief
        // window), or the sync-mode committed-marker deferral is enabled. The marker
        // is an *optional* openraft optimisation (re-derived from the log +
        // membership on restart), so deferring it never affects safety; it removes
        // the per-commit `state.json` fsync + dir-fsync. The log-append fsync (which
        // gates client acks) and the safety-critical purge marker are unaffected.
        match inner.async_durability() || inner.defer_committed {
            // Sync (no deferral): persist the committed marker durably now.
            false => Self::persist_state(&inner),
            // Deferred: the background flusher persists it within the flush cadence.
            true => {
                inner.state_dirty = true;
                Ok(())
            }
        }
    }

    async fn read_committed(&mut self) -> Result<Option<LogId<NodeId>>, StorageError<NodeId>> {
        Ok(self
            .inner
            .lock()
            .unwrap_or_else(|e| e.into_inner())
            .committed)
    }
}

#[cfg(test)]
mod tests {
    use openraft::storage::{RaftLogReader, RaftLogStorage};
    use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};

    use super::*;

    fn log_id(index: u64) -> LogId<NodeId> {
        LogId::new(CommittedLeaderId::new(1, 0), index)
    }

    fn entry(index: u64) -> Entry<RaftConfig> {
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Blank,
        }
    }

    /// Lays down a segment file `seg-<start>.ndjson` holding `indices`, exactly as
    /// the store would have written it, so `open`/`purge`/`truncate` can be driven
    /// without the `pub(crate)` `LogFlushed` append callback.
    fn write_segment(dir: &Path, start: u64, indices: &[u64]) {
        let mut bytes = Vec::new();
        for &i in indices {
            serde_json::to_writer(&mut bytes, &entry(i)).unwrap();
            bytes.push(b'\n');
        }
        std::fs::write(seg_path(dir, start), bytes).unwrap();
    }

    fn write_state(dir: &Path, last_purged: Option<u64>) {
        let state = PersistedState {
            last_purged: last_purged.map(log_id),
            committed: None,
        };
        std::fs::write(state_path(dir), serde_json::to_vec(&state).unwrap()).unwrap();
    }

    fn tmp_dir(tag: &str) -> PathBuf {
        let nanos = std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap()
            .as_nanos();
        let d = std::env::temp_dir().join(format!(
            "nanobpmn-logstore-{}-{tag}-{nanos}",
            std::process::id()
        ));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    async fn indices_in(store: &mut RaftLogStore) -> Vec<u64> {
        store
            .try_get_log_entries(..)
            .await
            .unwrap()
            .into_iter()
            .map(|e| e.log_id.index)
            .collect()
    }

    #[tokio::test]
    async fn open_filters_purged_entries_and_unlinks_fully_purged_segments() {
        let dir = tmp_dir("open-reconcile");
        // Three sealed segments; the durable marker says everything up to index 5 is
        // purged, so seg-0 (0..2) and seg-3 (3..5) are pure garbage.
        write_segment(&dir, 0, &[0, 1, 2]);
        write_segment(&dir, 3, &[3, 4, 5]);
        write_segment(&dir, 6, &[6, 7, 8]);
        write_state(&dir, Some(5));

        let mut store = RaftLogStore::open(&dir).unwrap();

        // Purged entries never enter the in-memory index.
        assert_eq!(indices_in(&mut store).await, vec![6, 7, 8]);
        let state = store.get_log_state().await.unwrap();
        assert_eq!(state.last_purged_log_id, Some(log_id(5)));
        assert_eq!(state.last_log_id, Some(log_id(8)));

        // The two fully-purged sealed segments were reclaimed on open; the live one
        // (also the active segment) survives.
        assert!(!seg_path(&dir, 0).exists(), "seg-0 should be unlinked");
        assert!(!seg_path(&dir, 3).exists(), "seg-3 should be unlinked");
        assert!(seg_path(&dir, 6).exists(), "seg-6 is live and must survive");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn purge_unlinks_sealed_segments_without_rewriting_the_live_one() {
        let dir = tmp_dir("purge");
        write_segment(&dir, 0, &[0, 1, 2]);
        write_segment(&dir, 3, &[3, 4, 5]);
        write_segment(&dir, 6, &[6, 7, 8]);

        let mut store = RaftLogStore::open(&dir).unwrap();
        assert_eq!(
            indices_in(&mut store).await,
            vec![0, 1, 2, 3, 4, 5, 6, 7, 8]
        );

        // Capture the live segment's bytes to prove purge never rewrites it.
        let live_before = std::fs::read(seg_path(&dir, 6)).unwrap();

        store.purge(log_id(5)).await.unwrap();

        // In-memory prefix dropped; the live segment file is byte-identical (no
        // rewrite), and the two fully-purged sealed segments are unlinked.
        assert_eq!(indices_in(&mut store).await, vec![6, 7, 8]);
        assert!(!seg_path(&dir, 0).exists());
        assert!(!seg_path(&dir, 3).exists());
        assert!(seg_path(&dir, 6).exists());
        assert_eq!(
            std::fs::read(seg_path(&dir, 6)).unwrap(),
            live_before,
            "purge must not rewrite the live segment"
        );

        // The purge marker is durable: a reopen sees the same trimmed log even
        // though seg-6 still physically contains no purged entries here.
        let mut reopened = RaftLogStore::open(&dir).unwrap();
        assert_eq!(indices_in(&mut reopened).await, vec![6, 7, 8]);
        assert_eq!(
            reopened.get_log_state().await.unwrap().last_purged_log_id,
            Some(log_id(5))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn purge_marker_survives_a_crash_before_physical_deletion() {
        // Simulate a crash mid-purge: the state marker was persisted (index 2) but
        // the segment file still physically holds 0..5. Reopen must NOT resurrect
        // the purged entries.
        let dir = tmp_dir("crash-purge");
        write_segment(&dir, 0, &[0, 1, 2, 3, 4, 5]);
        write_state(&dir, Some(2));

        let mut store = RaftLogStore::open(&dir).unwrap();
        assert_eq!(indices_in(&mut store).await, vec![3, 4, 5]);
        assert_eq!(
            store.get_log_state().await.unwrap().last_purged_log_id,
            Some(log_id(2))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn truncate_rewrites_only_the_straddled_segment() {
        let dir = tmp_dir("truncate");
        write_segment(&dir, 0, &[0, 1, 2]);
        write_segment(&dir, 3, &[3, 4, 5]);
        write_segment(&dir, 6, &[6, 7, 8]);

        let mut store = RaftLogStore::open(&dir).unwrap();
        let untouched_before = std::fs::read(seg_path(&dir, 0)).unwrap();

        // Conflict at index 4: everything >= 4 goes.
        store.truncate(log_id(4)).await.unwrap();

        assert_eq!(indices_in(&mut store).await, vec![0, 1, 2, 3]);
        // Segment starting at/after the cut is unlinked; the earlier untouched
        // segment is byte-identical; the straddled seg-3 was rewritten to just {3}.
        assert!(
            !seg_path(&dir, 6).exists(),
            "seg-6 (>=4) should be unlinked"
        );
        assert_eq!(
            std::fs::read(seg_path(&dir, 0)).unwrap(),
            untouched_before,
            "a segment entirely below the cut must not be rewritten"
        );
        assert!(seg_path(&dir, 3).exists());

        // A reopen reconstructs exactly the truncated log.
        let mut reopened = RaftLogStore::open(&dir).unwrap();
        assert_eq!(indices_in(&mut reopened).await, vec![0, 1, 2, 3]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Demotes all-but-the-newest entry (budget = 1 byte) and proves the reader
    /// transparently rehydrates the descriptor-only entries from their segment file
    /// — the whole point of the byte-bounded hot-window cache.
    #[tokio::test]
    async fn demoted_entries_are_rehydrated_from_disk_on_read() {
        let dir = tmp_dir("demote-read");
        write_segment(&dir, 0, &[0, 1, 2, 3, 4]);
        let mut store = RaftLogStore::open(&dir).unwrap();

        {
            let mut inner = store.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.ram_budget = 1;
            inner.demote_scan_from = 0;
            inner.enforce_ram_budget();
            // Everything but the newest entry (index 4) is demoted to descriptor-only.
            let resident: Vec<u64> = inner
                .log
                .iter()
                .filter(|(_, s)| s.entry.is_some())
                .map(|(i, _)| *i)
                .collect();
            assert_eq!(resident, vec![4], "only the newest entry stays resident");
            assert_eq!(
                inner.ram_bytes,
                inner.log.get(&4).unwrap().ram_len,
                "resident bytes collapse to just the hot entry"
            );
        }

        // Reads still return every entry, in order, reading the demoted ones back
        // from disk.
        assert_eq!(indices_in(&mut store).await, vec![0, 1, 2, 3, 4]);
        let sub = store.try_get_log_entries(1..4).await.unwrap();
        assert_eq!(
            sub.iter().map(|e| e.log_id.index).collect::<Vec<_>>(),
            vec![1, 2, 3]
        );
        // The last-log-id is served from the retained id even when demotion is in play.
        assert_eq!(
            store.get_log_state().await.unwrap().last_log_id,
            Some(log_id(4))
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `truncate` must handle demoted entries in the straddled segment: it keeps the
    /// surviving byte prefix without needing to rehydrate/re-serialize them.
    #[tokio::test]
    async fn truncate_with_demoted_entries_keeps_the_surviving_prefix() {
        let dir = tmp_dir("demote-truncate");
        write_segment(&dir, 0, &[0, 1, 2, 3, 4, 5]);
        let mut store = RaftLogStore::open(&dir).unwrap();
        {
            let mut inner = store.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.ram_budget = 1;
            inner.demote_scan_from = 0;
            inner.enforce_ram_budget();
        }

        store.truncate(log_id(3)).await.unwrap();
        assert_eq!(indices_in(&mut store).await, vec![0, 1, 2]);

        // The surviving prefix is durable and byte-correct across a reopen.
        let mut reopened = RaftLogStore::open(&dir).unwrap();
        assert_eq!(indices_in(&mut reopened).await, vec![0, 1, 2]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    /// `purge` must drop demoted prefix entries and keep resident-byte accounting in
    /// step (dropped entries were descriptor-only, so `ram_bytes` should not move).
    #[tokio::test]
    async fn purge_with_demoted_entries_drops_the_prefix() {
        let dir = tmp_dir("demote-purge");
        write_segment(&dir, 0, &[0, 1, 2]);
        write_segment(&dir, 3, &[3, 4, 5]);
        let mut store = RaftLogStore::open(&dir).unwrap();
        let ram_after_demote = {
            let mut inner = store.inner.lock().unwrap_or_else(|e| e.into_inner());
            inner.ram_budget = 1;
            inner.demote_scan_from = 0;
            inner.enforce_ram_budget();
            inner.ram_bytes
        };

        store.purge(log_id(2)).await.unwrap();
        assert_eq!(indices_in(&mut store).await, vec![3, 4, 5]);
        // The purged entries were demoted (descriptor-only), so resident bytes are
        // unchanged by the purge.
        assert_eq!(
            store
                .inner
                .lock()
                .unwrap_or_else(|e| e.into_inner())
                .ram_bytes,
            ram_after_demote
        );

        let mut reopened = RaftLogStore::open(&dir).unwrap();
        assert_eq!(indices_in(&mut reopened).await, vec![3, 4, 5]);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn open_migrates_a_legacy_single_file_log() {
        let dir = tmp_dir("legacy");
        // A pre-segmentation node left a `log.ndjson`.
        let mut bytes = Vec::new();
        for i in 0..4u64 {
            serde_json::to_writer(&mut bytes, &entry(i)).unwrap();
            bytes.push(b'\n');
        }
        std::fs::write(log_path(&dir), bytes).unwrap();

        let mut store = RaftLogStore::open(&dir).unwrap();

        assert_eq!(indices_in(&mut store).await, vec![0, 1, 2, 3]);
        assert!(
            !log_path(&dir).exists(),
            "legacy file should be migrated away"
        );
        assert!(
            seg_path(&dir, 0).exists(),
            "migrated into a start-0 segment"
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    // ---- Per-entry compression codec (NANOBPMN_RAFT_LOG_COMPRESS) ----

    /// Builds a large, compressible `Normal` entry (a `CreateInstance` carrying a
    /// business-like string payload) so the codec's size/shrink gates engage.
    fn big_entry(index: u64, payload_len: usize) -> Entry<RaftConfig> {
        use std::collections::HashMap;

        use nanobpmn_engine_core::{Command, Value};
        let mut s = String::with_capacity(payload_len + 32);
        while s.len() < payload_len {
            s.push_str("order-customer-invoice-amount-region ");
        }
        s.truncate(payload_len);
        let mut vars = HashMap::new();
        vars.insert("p".to_string(), Value::Str(s));
        let cmd = Command::create_instance_with("demo", vars);
        let batch = crate::raft::ReplicatedBatch::single(cmd, 12_345);
        Entry {
            log_id: log_id(index),
            payload: EntryPayload::Normal(batch),
        }
    }

    fn plaintext_record(index: u64) -> Vec<u8> {
        let mut b = Vec::new();
        serde_json::to_writer(&mut b, &entry(index)).unwrap();
        b.push(b'\n');
        b
    }

    fn framed_record(e: &Entry<RaftConfig>) -> (Vec<u8>, Vec<u8>) {
        let mut json = Vec::new();
        serde_json::to_writer(&mut json, e).unwrap();
        let frame = frame_entry(&json).expect("large compressible entry should frame");
        (frame, json)
    }

    #[test]
    fn frame_entry_roundtrips_and_shrinks_a_large_entry() {
        let e = big_entry(7, 50 * 1024);
        let (frame, json) = framed_record(&e);
        assert_eq!(frame[0], FRAME_MAGIC);
        assert_eq!(frame[1], CODEC_DEFLATE);
        assert!(
            frame.len() < json.len() + 1,
            "frame ({}) must shrink the plaintext write ({}+1)",
            frame.len(),
            json.len()
        );
        // Decoding the frame yields byte-identical entry JSON.
        let decoded = entry_from_record(&frame).unwrap();
        let mut got = Vec::new();
        serde_json::to_writer(&mut got, &decoded).unwrap();
        assert_eq!(got, json);
    }

    #[test]
    fn frame_entry_declines_a_small_entry() {
        let mut json = Vec::new();
        serde_json::to_writer(&mut json, &entry(3)).unwrap();
        assert!(
            frame_entry(&json).is_none(),
            "a tiny Blank entry must stay verbatim"
        );
    }

    #[test]
    fn entry_from_record_reads_both_plaintext_and_frames() {
        // Plaintext (with and without trailing newline) and a frame all decode.
        let pt = plaintext_record(5);
        assert_eq!(entry_from_record(&pt).unwrap().log_id.index, 5);
        assert_eq!(
            entry_from_record(&pt[..pt.len() - 1]).unwrap().log_id.index,
            5
        );
        let (frame, _) = framed_record(&big_entry(6, 20 * 1024));
        assert_eq!(entry_from_record(&frame).unwrap().log_id.index, 6);
    }

    #[test]
    fn scan_segment_walks_interleaved_plaintext_and_frames() {
        let dir = tmp_dir("scan-interleave");
        let r0 = plaintext_record(0);
        let (r1, _) = framed_record(&big_entry(1, 40 * 1024));
        let r2 = plaintext_record(2);
        let mut file = Vec::new();
        file.extend_from_slice(&r0);
        file.extend_from_slice(&r1);
        file.extend_from_slice(&r2);
        std::fs::write(seg_path(&dir, 0), &file).unwrap();

        let (records, total) = scan_segment(&seg_path(&dir, 0)).unwrap();
        assert_eq!(total, file.len(), "total bytes = physical file size");
        assert_eq!(
            records.iter().map(|r| r.index).collect::<Vec<_>>(),
            vec![0, 1, 2]
        );
        // Physical, contiguous offsets — the invariant random-access reads rely on.
        assert_eq!(records[0].offset, 0);
        assert_eq!(records[0].len, r0.len());
        assert_eq!(records[1].offset, r0.len());
        assert_eq!(records[1].len, r1.len());
        assert_eq!(records[2].offset, r0.len() + r1.len());
        // Reading each record's exact byte span decodes to the right entry — exactly
        // what `read_entry_at` does with a demoted entry's (offset, len).
        for r in &records {
            let span = &file[r.offset..r.offset + r.len];
            assert_eq!(entry_from_record(span).unwrap().log_id.index, r.index);
        }
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn scan_segment_errors_on_a_torn_frame_tail() {
        let dir = tmp_dir("scan-torn");
        let (frame, _) = framed_record(&big_entry(1, 30 * 1024));
        // Drop the last 100 body bytes to simulate a torn write.
        let torn = &frame[..frame.len() - 100];
        std::fs::write(seg_path(&dir, 0), torn).unwrap();
        assert!(
            scan_segment(&seg_path(&dir, 0)).is_err(),
            "a truncated frame body must be rejected, not silently accepted"
        );
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[tokio::test]
    async fn open_recovers_a_segment_mixing_plaintext_and_frames() {
        let dir = tmp_dir("open-compressed");
        let r0 = plaintext_record(0);
        let (r1, _) = framed_record(&big_entry(1, 50 * 1024));
        let (r2, _) = framed_record(&big_entry(2, 50 * 1024));
        let mut file = Vec::new();
        file.extend_from_slice(&r0);
        file.extend_from_slice(&r1);
        file.extend_from_slice(&r2);
        std::fs::write(seg_path(&dir, 0), &file).unwrap();

        let mut store = RaftLogStore::open(&dir).unwrap();
        assert_eq!(indices_in(&mut store).await, vec![0, 1, 2]);
        // The recovered entries read back intact (payload survives the codec).
        let got = store.try_get_log_entries(1..=2).await.unwrap();
        assert_eq!(got.len(), 2);
        assert!(matches!(got[0].payload, EntryPayload::Normal(_)));
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// Regression for the compressed-log RAM-accounting bug: a compressed frame is
    /// small on disk but its resident `Entry` is uncompressed, so `ram_bytes` must
    /// reflect the *uncompressed* footprint — otherwise the RAM budget never demotes
    /// and `nanobpm_raft_log_ram_bytes` undercounts resident memory by the
    /// compression ratio (the observed ~5 GB unaccounted heap under 50 KB payloads).
    #[tokio::test]
    async fn ram_bytes_accounts_uncompressed_size_for_framed_entries() {
        let dir = tmp_dir("ram-uncompressed");
        let (r0, j0) = framed_record(&big_entry(0, 50 * 1024));
        let (r1, j1) = framed_record(&big_entry(1, 50 * 1024));
        let mut file = Vec::new();
        file.extend_from_slice(&r0);
        file.extend_from_slice(&r1);
        std::fs::write(seg_path(&dir, 0), &file).unwrap();

        let store = RaftLogStore::open(&dir).unwrap();
        let inner = store.inner.lock().unwrap_or_else(|e| e.into_inner());
        let on_disk = r0.len() + r1.len();
        let uncompressed = (j0.len() + 1) + (j1.len() + 1);
        assert_eq!(
            inner.log_bytes, on_disk,
            "log_bytes tracks the compressed on-disk footprint"
        );
        assert_eq!(
            inner.ram_bytes, uncompressed,
            "ram_bytes tracks the uncompressed resident footprint"
        );
        assert!(
            inner.ram_bytes > inner.log_bytes * 2,
            "a 50 KB compressible payload must cost far more resident RAM ({}) \
             than its compressed on-disk record ({})",
            inner.ram_bytes,
            inner.log_bytes
        );
        drop(inner);
        let _ = std::fs::remove_dir_all(&dir);
    }

    /// The recovery fsync-relief gate: coalescing is opt-in (default OFF, fully
    /// durable), and even when enabled it only defers a `sync` store's fsync
    /// *while the recovery window is engaged*, reverting on clear. The vote
    /// (always synchronous elsewhere) is untouched by this flag. Both behaviours
    /// live in one test because they share the process-global relief flag, which
    /// would race across parallel test functions.
    #[tokio::test]
    async fn recovery_relief_gates_fsync_and_is_off_by_default() {
        // --- Default posture: coalescing off => fsync stays inline even in the
        // recovery window (durability fully preserved). ---
        let off_dir = tmp_dir("relief-default-off");
        let off_store = RaftLogStore::open(&off_dir).unwrap();
        set_recovery_fsync_relief(true);
        assert!(
            !off_store.would_defer_fsync(),
            "a default (coalescing-off) sync store must fsync inline even during recovery"
        );

        // --- Opt-in posture: a coalesce-capable sync store defers only while the
        // recovery window is engaged, and reverts on clear. ---
        let on_dir = tmp_dir("relief-defer");
        let on_store = RaftLogStore::open(&on_dir).unwrap();
        on_store.force_coalesce_capable(true);

        set_recovery_fsync_relief(false);
        assert!(
            !on_store.would_defer_fsync(),
            "a steady sync store must fsync inline (relief off)"
        );
        set_recovery_fsync_relief(true);
        assert!(
            on_store.would_defer_fsync(),
            "a coalesce-capable sync store must defer fsync while relief is engaged"
        );
        set_recovery_fsync_relief(false);
        assert!(
            !on_store.would_defer_fsync(),
            "relief clearing must restore inline fsync"
        );

        let _ = std::fs::remove_dir_all(&off_dir);
        let _ = std::fs::remove_dir_all(&on_dir);
    }

    /// The sync-mode committed-marker deferral: OFF by default (every commit advance
    /// rewrites `state.json` inline), and when enabled the marker is held in memory
    /// (`state_dirty`) and only persisted by the background flusher — without
    /// relaxing log-append durability. On restart the deferred marker is re-derived
    /// from the log, so a not-yet-flushed committed advance is never a safety loss.
    #[tokio::test]
    async fn defer_committed_marker_defers_the_state_json_fsync_when_enabled() {
        use openraft::storage::RaftLogStorage;

        // --- Default posture: deferral off => save_committed persists inline. ---
        let off_dir = tmp_dir("defer-committed-off");
        let mut off_store = RaftLogStore::open(&off_dir).unwrap();
        off_store.save_committed(Some(log_id(7))).await.unwrap();
        assert!(
            !off_store.is_state_dirty(),
            "a default sync store must persist the committed marker inline (not dirty)"
        );
        let persisted: PersistedState = read_json(&state_path(&off_dir)).unwrap().unwrap();
        assert_eq!(
            persisted.committed,
            Some(log_id(7)),
            "inline persist must land the committed marker on disk immediately"
        );

        // --- Opt-in posture: deferral on => marker stays in memory until the
        // background flusher persists it. ---
        let on_dir = tmp_dir("defer-committed-on");
        let mut on_store = RaftLogStore::open(&on_dir).unwrap();
        on_store.force_defer_committed(true);
        on_store.save_committed(Some(log_id(9))).await.unwrap();
        assert!(
            on_store.is_state_dirty(),
            "a deferring sync store must hold the committed marker dirty, not fsync it inline"
        );
        let on_disk: Option<PersistedState> = read_json(&state_path(&on_dir)).unwrap();
        assert!(
            on_disk.map(|s| s.committed).unwrap_or(None).is_none(),
            "the committed marker must NOT be on disk before the flusher runs"
        );
        // The background flusher persists it and clears the dirty flag.
        on_store.flush_now().unwrap();
        assert!(
            !on_store.is_state_dirty(),
            "flush must clear the dirty flag"
        );
        let flushed: PersistedState = read_json(&state_path(&on_dir)).unwrap().unwrap();
        assert_eq!(
            flushed.committed,
            Some(log_id(9)),
            "the flusher must persist the deferred committed marker"
        );

        let _ = std::fs::remove_dir_all(&off_dir);
        let _ = std::fs::remove_dir_all(&on_dir);
    }
}
