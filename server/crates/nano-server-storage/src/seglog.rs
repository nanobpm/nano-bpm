//! Segmented engine journal with periodic snapshots and compaction — the
//! bounded-disk durability format for the **single-partition** persistent path.
//!
//! # Why
//!
//! The legacy journal ([`crate::journal`]) is one append-only `journal.jsonl`
//! that is never truncated, and boot recovery replays it in full. That makes
//! on-disk size grow without bound on a long-running workload, and there is no
//! safe way to truncate the *prefix* of a file the writer thread is actively
//! appending to. This module solves both with the standard log-structured
//! approach (Zeebe / Raft / Kafka):
//!
//! - The log is a **directory of segments**. The active segment keeps the
//!   historical name `journal.jsonl` (so an existing single-file data dir is
//!   adopted as-is). When it grows past `NANOBPMN_JOURNAL_SEGMENT_BYTES` — or
//!   when a snapshot rotates it — it is *sealed*: renamed to
//!   `journal.seg.<startIndex>.jsonl`, where `startIndex` is the absolute index
//!   of its first event, and a fresh empty `journal.jsonl` is opened.
//! - Periodically the engine's compact [`EngineSnapshot`] is persisted to
//!   `snapshot.<coveredEvents>.bin` after rotating the active segment, so the
//!   snapshot covers exactly the events in the sealed segments.
//! - **Compaction** deletes any sealed segment whose events are *both* covered
//!   by the latest snapshot *and* already projected into the read model (the
//!   `exported_position` watermark — the Zeebe exporter bound). The active
//!   segment is never touched.
//! - Boot = load the latest snapshot, then replay only the events the snapshot
//!   did not cover (the surviving sealed tail + the active segment).
//!
//! Every segment's start index is encoded in its filename, so absolute event
//! positions survive a crash at any point without a separate index file (the
//! one optional `journal.head` file only records the active segment's start so
//! it is recoverable when *all* sealed segments have been compacted away).

use std::fs::{self, File, OpenOptions};
use std::io::{self, BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};

use nanobpmn_engine_core::{
    Engine, EngineSnapshot, Event, EventDecodeError, SNAPSHOT_FORMAT_VERSION, decode_event_json,
    partition_of,
};

/// The active segment keeps the historical journal name so an existing
/// single-file data dir is adopted unchanged.
pub const ACTIVE_NAME: &str = "journal.jsonl";
const SEG_PREFIX: &str = "journal.seg.";
const SEG_SUFFIX: &str = ".jsonl";
const SNAP_PREFIX: &str = "snapshot.";
const SNAP_SUFFIX: &str = ".bin";
const HEAD_NAME: &str = "journal.head";
/// Combined per-partition snapshot for the multi-partition shared-WAL path.
const MULTI_SNAP_NAME: &str = "msnapshot.bin";
/// Per-partition head: the active segment's per-partition cumulative start counts
/// (recovers `per_partition_active_start` when every sealed segment is compacted).
const PPHEAD_NAME: &str = "journal.pphead";
/// Per-sealed-segment sidecar suffix carrying that segment's per-partition
/// cumulative START counts (its `end` counts are derived by demuxing on recovery).
const PP_META_SUFFIX: &str = ".ppmeta";

/// Default seal threshold for the active segment (128 MiB). Tunable via
/// `NANOBPMN_JOURNAL_SEGMENT_BYTES`; `0` disables size-based sealing (segments
/// then roll only when a snapshot rotates them).
const DEFAULT_SEGMENT_BYTES: u64 = 128 << 20;

/// Seal threshold from `NANOBPMN_JOURNAL_SEGMENT_BYTES` (bytes). `0` = no
/// size-based sealing. A finite value, clamped to a 1 MiB floor so a tiny value
/// can't seal every batch, otherwise the default.
pub fn segment_bytes_from_env() -> u64 {
    match std::env::var("NANOBPMN_JOURNAL_SEGMENT_BYTES") {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(0) => u64::MAX, // size-based sealing off
            Ok(n) => n.max(1 << 20),
            Err(_) => DEFAULT_SEGMENT_BYTES,
        },
        Err(_) => DEFAULT_SEGMENT_BYTES,
    }
}

/// Whether the segmented journal should frame-compress its durable writes.
/// Off by default; set `NANOBPMN_JOURNAL_COMPRESS=1` to enable. This is the
/// "value codec": the writer deflates each group-commit batch *before* it hits
/// the active segment, shrinking the physical write bandwidth that bounds
/// large-payload (50 KB-class) throughput — the 264 MB/s per-node disk-write
/// wall. Only the *segmented* path honours it; the legacy single-file journal
/// always writes plaintext.
pub fn journal_compress_from_env() -> bool {
    matches!(
        std::env::var("NANOBPMN_JOURNAL_COMPRESS").as_deref(),
        Ok("1") | Ok("true") | Ok("on") | Ok("yes")
    )
}

/// Frame magic: ASCII RS (record separator, `0x1E`). A plaintext journal record
/// always begins with a digit (`<partition>\t…`) or `{` (bare event JSON), never
/// `0x1E`, so a reader can tell a compressed frame from a legacy line by looking
/// at one byte — which lets a single segment freely interleave the two (the
/// compression flag can flip across a restart while the same active segment is
/// still open).
const FRAME_MAGIC: u8 = 0x1E;
/// Frame carries the batch verbatim (compression declined but framing kept).
const CODEC_RAW: u8 = 0;
/// Frame body is raw-deflate (mirrors the Raft wire codec in `raft_net.rs`).
const CODEC_DEFLATE: u8 = 1;
/// Frame header: `magic(1) | codec(1) | raw_len(u32 LE) | comp_len(u32 LE)`.
const FRAME_HEADER_LEN: usize = 10;
/// Don't frame batches below this — the header + deflate cost isn't worth it,
/// and (crucially) the negligible/high-rate regime commits small batches that
/// must stay verbatim so the single journal-writer thread is never taxed.
const MIN_FRAME_BYTES: usize = 16 * 1024;
/// Only compress when the batch's *mean* event is at least this big. This gates
/// compression to the big-payload regime and skips high-rate small-event
/// batches even when they aggregate past `MIN_FRAME_BYTES`.
const MIN_AVG_EVENT_BYTES: usize = 1024;

/// Frames a group-commit `buf` for durable append, deflating it when it is worth
/// it. Returns `None` when the batch should be written verbatim (too small, mean
/// event too small, deflate failed, or the result didn't actually shrink it) —
/// mixing framed and plaintext records in one segment is expected and the reader
/// tolerates it. Never errors: compression is best-effort, durability is not.
fn frame_compress(buf: &[u8], events: u64) -> Option<Vec<u8>> {
    if buf.len() < MIN_FRAME_BYTES {
        return None;
    }
    if buf.len() / (events.max(1) as usize) < MIN_AVG_EVENT_BYTES {
        return None;
    }
    use std::io::Write;

    use flate2::{Compression, write::DeflateEncoder};
    let mut enc = DeflateEncoder::new(Vec::with_capacity(buf.len() / 2), Compression::fast());
    if enc.write_all(buf).is_err() {
        return None;
    }
    let comp = enc.finish().ok()?;
    // Only worth a frame if it meaningfully shrinks the physical write.
    if comp.len().checked_add(FRAME_HEADER_LEN)? >= buf.len() {
        return None;
    }
    let raw_len = u32::try_from(buf.len()).ok()?;
    let comp_len = u32::try_from(comp.len()).ok()?;
    let mut frame = Vec::with_capacity(FRAME_HEADER_LEN + comp.len());
    frame.push(FRAME_MAGIC);
    frame.push(CODEC_DEFLATE);
    frame.extend_from_slice(&raw_len.to_le_bytes());
    frame.extend_from_slice(&comp_len.to_le_bytes());
    frame.extend_from_slice(&comp);
    Some(frame)
}

/// Decodes a segment file into its logical plaintext bytes — the concatenated
/// newline-terminated records the writer appended — transparently inflating any
/// compressed frames. Walks the file record-by-record: a [`FRAME_MAGIC`] byte
/// begins a frame, anything else begins a legacy plaintext line, so a segment
/// may freely interleave the two. A torn trailing frame header/body (a crash
/// mid-append that was never fsynced, hence never acked) is dropped, matching
/// the writer's ack-after-write-before-fsync durability contract.
fn decode_segment_bytes(path: &Path) -> io::Result<Vec<u8>> {
    let raw = fs::read(path)?;
    let mut out: Vec<u8> = Vec::with_capacity(raw.len());
    let mut i = 0;
    while i < raw.len() {
        if raw[i] == FRAME_MAGIC {
            if i + FRAME_HEADER_LEN > raw.len() {
                break; // torn header tail — nothing durable past here
            }
            let codec = raw[i + 1];
            let raw_len = u32::from_le_bytes(raw[i + 2..i + 6].try_into().unwrap()) as usize;
            let comp_len = u32::from_le_bytes(raw[i + 6..i + 10].try_into().unwrap()) as usize;
            let start = i + FRAME_HEADER_LEN;
            let Some(end) = start.checked_add(comp_len).filter(|e| *e <= raw.len()) else {
                break; // torn frame body
            };
            let payload = &raw[start..end];
            match codec {
                CODEC_RAW => out.extend_from_slice(payload),
                CODEC_DEFLATE => {
                    use std::io::Read;

                    use flate2::read::DeflateDecoder;
                    let before = out.len();
                    DeflateDecoder::new(payload).read_to_end(&mut out)?;
                    if out.len() - before != raw_len {
                        return Err(io::Error::new(
                            io::ErrorKind::InvalidData,
                            "journal frame inflated to unexpected length",
                        ));
                    }
                }
                other => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!("unknown journal frame codec {other}"),
                    ));
                }
            }
            i = end;
        } else {
            // Legacy plaintext record: copy through the next newline (inclusive).
            // An unterminated trailing line is a crash mid-append that was never
            // newline-terminated — hence never fsynced/acked — so it is dropped,
            // matching the torn-frame contract above and the writer's
            // ack-after-write-before-fsync durability contract. Copying it through
            // would hand a truncated JSON record to the segment reader and panic
            // boot recovery ("EOF while parsing a string").
            match raw[i..].iter().position(|&b| b == b'\n') {
                Some(nl) => {
                    out.extend_from_slice(&raw[i..=i + nl]);
                    i += nl + 1;
                }
                None => break,
            }
        }
    }
    Ok(out)
}

/// Whether the segmented journal is enabled for the persistent single-partition
/// path. On by default; set `NANOBPMN_JOURNAL_SEGMENTED=0` to fall back to the
/// legacy single-file journal (no compaction).
pub fn segmented_enabled() -> bool {
    match std::env::var("NANOBPMN_JOURNAL_SEGMENTED") {
        Ok(v) => !matches!(v.trim(), "0" | "false" | "off" | "no"),
        Err(_) => true,
    }
}

/// Opt-in escape hatch for the unrecoverable read-model compaction gap (see
/// [`CatchUpPlan::CompactedGap`] and [`catch_up_shard`]). When set truthy via
/// `NANOBPMN_READ_MODEL_LOSSY_REBUILD`, a boot that finds the read model below
/// the journal's compaction floor rebuilds it from the surviving journal tail —
/// **permanently dropping** the compacted-away history — instead of aborting.
/// Default (`false`) fails fast so the data loss is never silent.
pub fn read_model_lossy_rebuild_enabled() -> bool {
    match std::env::var("NANOBPMN_READ_MODEL_LOSSY_REBUILD") {
        Ok(v) => matches!(v.trim(), "1" | "true" | "on" | "yes"),
        Err(_) => false,
    }
}

/// Snapshot + compaction cadence from `NANOBPMN_SNAPSHOT_INTERVAL_MS` (default
/// 60 s, floored at 1 s). `0` disables periodic snapshots/compaction entirely
/// (the journal then grows unbounded, as in the legacy path). Returns `None`
/// when disabled.
pub fn snapshot_interval_from_env() -> Option<std::time::Duration> {
    match std::env::var("NANOBPMN_SNAPSHOT_INTERVAL_MS") {
        Ok(v) => match v.trim().parse::<u64>() {
            Ok(0) => None,
            Ok(ms) => Some(std::time::Duration::from_millis(ms.max(1000))),
            Err(_) => Some(std::time::Duration::from_secs(60)),
        },
        Err(_) => Some(std::time::Duration::from_secs(60)),
    }
}

/// A sealed (immutable) segment: the absolute event-index range it covers and
/// the file backing it.
#[derive(Clone, Debug)]
pub struct SealedSeg {
    /// Absolute index of this segment's first event.
    pub start: u64,
    /// Absolute index one past this segment's last event (== next segment start).
    pub end: u64,
    pub path: PathBuf,
    /// Per-partition cumulative event count at this segment's END, indexed by
    /// global partition id. Empty for the single-partition (legacy) path. Used
    /// by multi-partition compaction: a segment is deletable only once every
    /// partition `p`'s snapshot covers `per_partition_end[p]`.
    pub per_partition_end: Vec<u64>,
}

/// The boundary produced by sealing the active segment: the absolute event
/// count it now covers (`== next segment start`), plus (multi-partition only)
/// the per-partition cumulative counts at that boundary.
#[derive(Clone, Debug)]
pub struct SealInfo {
    pub end: u64,
    /// Per-partition cumulative event count at the seal boundary, indexed by
    /// global partition id. Empty for the single-partition (legacy) path. The
    /// snapshot maintenance tick reads this partition's entry as its snapshot's
    /// covered count.
    pub per_partition_end: Vec<u64>,
}

/// State shared between the writer thread (which seals segments) and the
/// snapshot/compaction maintenance task (which reads boundaries and deletes
/// covered segments). Lives in an `Arc`.
pub struct SegShared {
    /// Directory holding the segments, snapshots and head file.
    pub dir: PathBuf,
    /// Absolute path of the active segment (`<dir>/journal.jsonl`).
    pub active_path: PathBuf,
    /// Cumulative count of events durably appended across all segments
    /// (sealed + active). Advanced by the writer after each batch.
    pub total_events: AtomicU64,
    /// Absolute index of the active segment's first event (advanced on each seal).
    pub active_start: AtomicU64,
    /// Sealed segments, ascending by `start`. Mutated by the writer on seal and
    /// by compaction on delete.
    pub sealed: Mutex<Vec<SealedSeg>>,
    /// Active-segment seal threshold in bytes (`u64::MAX` disables it).
    pub segment_bytes: u64,
    /// Per-partition cumulative event count across all segments (sealed +
    /// active), indexed by global partition id. Advanced by the writer per
    /// batch. Empty for the single-partition (legacy) path — its presence is
    /// what makes the writer track partitions and seal-time sidecars.
    pub per_partition_total: Vec<AtomicU64>,
    /// Per-partition cumulative count at the ACTIVE segment's first event
    /// (advanced on each seal), indexed by global partition id. Persisted to
    /// [`PPHEAD_NAME`] so `base_p` is recoverable when every sealed segment has
    /// been compacted away. Empty for the single-partition path.
    pub per_partition_active_start: Vec<AtomicU64>,
    /// Whether the writer frame-compresses group-commit batches before they hit
    /// the active segment (the value codec). Set from
    /// [`journal_compress_from_env`] on the segmented path; always `false` on the
    /// legacy single-file path.
    pub compress: bool,
}

impl SegShared {
    /// A non-segmenting shared state for the legacy single-file paths
    /// (`Journal::open_partition`, `SharedWriter`): an infinite seal threshold
    /// means the writer never rotates, so the file keeps its given path and
    /// behaves exactly as before. The active path is the caller's journal file.
    pub fn legacy(active_path: PathBuf) -> Arc<Self> {
        let dir = active_path
            .parent()
            .map(Path::to_path_buf)
            .unwrap_or_else(|| PathBuf::from("."));
        Arc::new(Self {
            dir,
            active_path,
            total_events: AtomicU64::new(0),
            active_start: AtomicU64::new(0),
            sealed: Mutex::new(Vec::new()),
            segment_bytes: u64::MAX,
            per_partition_total: Vec::new(),
            per_partition_active_start: Vec::new(),
            compress: false,
        })
    }

    fn sealed_name(&self, start: u64) -> PathBuf {
        self.dir
            .join(format!("{SEG_PREFIX}{start:020}{SEG_SUFFIX}"))
    }

    fn head_path(&self) -> PathBuf {
        self.dir.join(HEAD_NAME)
    }

    /// Number of partitions this log tracks (0 for the single-partition legacy
    /// path, which does no per-partition bookkeeping).
    pub fn partitions(&self) -> usize {
        self.per_partition_total.len()
    }

    /// Advances the per-partition cumulative counts by `deltas` (indexed by
    /// global partition id). A no-op for the legacy path (empty vectors).
    pub fn add_partition_events(&self, deltas: &[u64]) {
        for (slot, delta) in self.per_partition_total.iter().zip(deltas) {
            if *delta != 0 {
                slot.fetch_add(*delta, Ordering::Release);
            }
        }
    }

    /// Snapshot of the current per-partition cumulative totals.
    fn per_partition_totals(&self) -> Vec<u64> {
        self.per_partition_total
            .iter()
            .map(|c| c.load(Ordering::Acquire))
            .collect()
    }

    /// Snapshot of the per-partition active-segment start counts.
    fn per_partition_starts(&self) -> Vec<u64> {
        self.per_partition_active_start
            .iter()
            .map(|c| c.load(Ordering::Acquire))
            .collect()
    }

    fn pp_meta_path(&self, start: u64) -> PathBuf {
        self.dir.join(format!(
            "{SEG_PREFIX}{start:020}{SEG_SUFFIX}{PP_META_SUFFIX}"
        ))
    }

    fn pphead_path(&self) -> PathBuf {
        self.dir.join(PPHEAD_NAME)
    }
}

/// The active segment owned by the writer thread: the open append handle plus
/// the bookkeeping needed to seal it. Used by both writer loops (legacy and
/// segmented) so segmentation is transparent to the group-commit machinery; the
/// legacy path simply uses `segment_bytes == u64::MAX` and never rotates.
pub struct ActiveSegment {
    shared: Arc<SegShared>,
    file: File,
    bytes: u64,
}

impl ActiveSegment {
    /// Opens (creating, positioned to append) the active segment for `shared`.
    pub fn open(shared: Arc<SegShared>) -> io::Result<Self> {
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&shared.active_path)?;
        let bytes = file.metadata().map(|m| m.len()).unwrap_or(0);
        Ok(Self {
            shared,
            file,
            bytes,
        })
    }

    /// Access to the shared seal/boundary state, for the writer loop to bump
    /// per-partition counters and consult segment boundaries.
    pub fn shared(&self) -> &Arc<SegShared> {
        &self.shared
    }

    /// Appends a group-committed batch of `events` (already serialized to
    /// newline-terminated `buf`) to the active segment. When the shared state
    /// has `compress` set, the batch is frame-compressed first (see
    /// [`frame_compress`]) so the *physical* write — the disk-bandwidth wall for
    /// large payloads — shrinks; `self.bytes` therefore tracks on-disk bytes, so
    /// size-based sealing rotates on physical size.
    pub fn write_all(&mut self, buf: &[u8], events: u64) -> io::Result<()> {
        let written = if self.shared.compress
            && let Some(frame) = frame_compress(buf, events)
        {
            self.file.write_all(&frame)?;
            frame.len() as u64
        } else {
            self.file.write_all(buf)?;
            buf.len() as u64
        };
        self.bytes += written;
        self.shared
            .total_events
            .fetch_add(events, Ordering::Release);
        Ok(())
    }

    /// Forces a durability barrier on the active segment.
    pub fn fsync(&mut self) -> io::Result<()> {
        self.file.sync_all()
    }

    /// Seals the active segment if it has grown past the size threshold,
    /// returning the boundary when a seal happened.
    pub fn maybe_seal(&mut self) -> io::Result<Option<SealInfo>> {
        if self.bytes >= self.shared.segment_bytes && self.bytes > 0 {
            Ok(Some(self.seal()?))
        } else {
            Ok(None)
        }
    }

    /// Seals the active segment: fsync, rename it to its sealed name (keyed by
    /// its start index), record the boundary, persist the new active start, and
    /// open a fresh empty active segment. Returns the sealed boundary.
    pub fn seal(&mut self) -> io::Result<SealInfo> {
        // Flush everything we are about to make immutable.
        self.file.sync_all()?;

        let start = self.shared.active_start.load(Ordering::Acquire);
        let end = self.shared.total_events.load(Ordering::Acquire);
        // Per-partition boundary counts (empty on the single-partition path).
        let per_partition_end = self.shared.per_partition_totals();
        let per_partition_start = self.shared.per_partition_starts();

        // An empty active segment has nothing to seal; just report the boundary.
        if end == start {
            return Ok(SealInfo {
                end,
                per_partition_end,
            });
        }

        let sealed_path = self.shared.sealed_name(start);
        // Persist this segment's per-partition START counts (its END is derived
        // by demuxing on recovery) BEFORE the rename is made durable, so a
        // surviving segment always has its sidecar for `base_p` recovery.
        if !per_partition_start.is_empty() {
            write_pp_meta(&self.shared.pp_meta_path(start), &per_partition_start);
        }
        fs::rename(&self.shared.active_path, &sealed_path)?;

        // Open a fresh active segment and make the rename durable.
        self.file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(&self.shared.active_path)?;
        self.bytes = 0;
        fsync_dir(&self.shared.dir);

        self.shared.active_start.store(end, Ordering::Release);
        write_head(&self.shared.head_path(), end);
        // Advance the per-partition active start to this seal's end and persist
        // it, so `base_p` survives even once every sealed segment is compacted.
        if !per_partition_end.is_empty() {
            for (slot, v) in self
                .shared
                .per_partition_active_start
                .iter()
                .zip(&per_partition_end)
            {
                slot.store(*v, Ordering::Release);
            }
            write_pphead(&self.shared.pphead_path(), &per_partition_end);
        }

        self.shared
            .sealed
            .lock()
            .expect("sealed lock")
            .push(SealedSeg {
                start,
                end,
                path: sealed_path,
                per_partition_end: per_partition_end.clone(),
            });

        Ok(SealInfo {
            end,
            per_partition_end,
        })
    }
}

/// fsync a directory so a contained rename/create is durable. Best-effort:
/// directory fsync is unsupported on some platforms (e.g. Windows), where the
/// rename is durable by other means.
fn fsync_dir(dir: &Path) {
    if let Ok(f) = File::open(dir) {
        let _ = f.sync_all();
    }
}

/// Atomically (tmp + rename) writes the active-segment start index to the head
/// file, so it is recoverable even when every sealed segment has been compacted.
fn write_head(path: &Path, active_start: u64) {
    let tmp = path.with_extension("head.tmp");
    if fs::write(&tmp, active_start.to_string().as_bytes()).is_ok() {
        let _ = fs::rename(&tmp, path);
    }
}

fn read_head(path: &Path) -> Option<u64> {
    fs::read_to_string(path).ok()?.trim().parse::<u64>().ok()
}

/// Serializes a per-partition count vector as a compact comma-separated line.
fn encode_counts(counts: &[u64]) -> String {
    counts
        .iter()
        .map(u64::to_string)
        .collect::<Vec<_>>()
        .join(",")
}

fn decode_counts(s: &str) -> Option<Vec<u64>> {
    let s = s.trim();
    if s.is_empty() {
        return Some(Vec::new());
    }
    s.split(',').map(|p| p.trim().parse::<u64>().ok()).collect()
}

/// Atomically writes a sealed segment's per-partition START counts sidecar.
fn write_pp_meta(path: &Path, per_partition_start: &[u64]) {
    let tmp = path.with_extension("ppmeta.tmp");
    if fs::write(&tmp, encode_counts(per_partition_start).as_bytes()).is_ok() {
        let _ = fs::rename(&tmp, path);
    }
}

fn read_pp_meta(path: &Path) -> Option<Vec<u64>> {
    decode_counts(&fs::read_to_string(path).ok()?)
}

/// Atomically writes the active segment's per-partition start counts, so
/// `base_p` is recoverable when every sealed segment has been compacted.
fn write_pphead(path: &Path, per_partition_active_start: &[u64]) {
    let tmp = path.with_extension("pphead.tmp");
    if fs::write(&tmp, encode_counts(per_partition_active_start).as_bytes()).is_ok() {
        let _ = fs::rename(&tmp, path);
    }
}

fn read_pphead(path: &Path) -> Option<Vec<u64>> {
    decode_counts(&fs::read_to_string(path).ok()?)
}

/// A persisted snapshot: the engine's compact state plus the absolute event
/// count it covers (== the sealed-segment boundary at snapshot time).
#[derive(serde::Serialize, serde::Deserialize)]
struct PersistedSnapshot {
    covered_events: u64,
    engine: EngineSnapshot,
}

// ----------------------------------------------------------------------------
// Versioned, self-describing snapshot envelope (L2 / #1068).
//
// Historically a `snapshot.*.bin` / `msnapshot.bin` was bare `serde_json` of the
// payload struct with no version marker: there was no way to tell which format a
// file was — or that this build cannot read it — without attempting a full
// deserialize (and a failed deserialize was silently swallowed via `.ok()?`,
// which could rewind durable state — the #1065 incident).
//
// The new layout prepends an ALWAYS-parseable, minimal, single-line JSON
// **header** terminated by `\n`, then the existing serialized payload:
//
//     {"format_version":1,"incarnation":<u64>,"engine_fingerprint":"<str>"}\n
//     <payload json bytes...>
//
// The header shape is FIXED FOR ALL TIME (never add/rename a header field) so
// any future build can read `format_version` without deserializing the payload
// body. Compact `serde_json` never emits a raw `0x0A`, so the first `\n` is an
// unambiguous header/payload delimiter and its ABSENCE marks a legacy headerless
// file (treated as `format_version = 0`).
// ----------------------------------------------------------------------------

/// Data-dir file holding this durable lifetime's incarnation id (see
/// [`SnapshotHeader::incarnation`]). Generated once per data dir and stamped
/// into every snapshot header.
const INCARNATION_NAME: &str = "journal.incarnation";

/// The always-parseable envelope header prefixed to every persisted snapshot.
///
/// Its shape MUST never change — a reader of any future format version has to be
/// able to parse this to learn the `format_version` before it decides whether it
/// can read the payload at all.
#[derive(serde::Serialize, serde::Deserialize)]
struct SnapshotHeader {
    /// The [`SNAPSHOT_FORMAT_VERSION`] the payload was serialized at. `0` denotes
    /// a legacy headerless file (synthesised on read; never written).
    format_version: u32,
    /// Monotonic epoch id of this data directory's durable lifetime, persisted in
    /// [`INCARNATION_NAME`] and regenerated when the data dir is reset/restored.
    /// Consumed cross-repo by nano-workforce#622 to reconcile after reset/restore
    /// (blackboard contract `snapshot-envelope-header-v1`).
    incarnation: u64,
    /// Coarse engine build identity (engine-core crate version) — diagnostic
    /// only. The authoritative serialized-shape drift fingerprint is #1069's
    /// concern and is a separate mechanism.
    engine_fingerprint: String,
}

/// A typed, FATAL failure to load a present snapshot file. Distinct from
/// `Ok(None)`, which means ONLY "no snapshot file exists". A present snapshot is
/// never silently ignored: an incompatible or corrupt file aborts recovery
/// (fail-closed) rather than rewinding to an earlier state.
///
/// Delivered to callers wrapped in an [`io::Error`] of kind
/// [`io::ErrorKind::InvalidData`]; recover it with
/// [`io::Error::get_ref`]/[`downcast_ref`](std::error::Error) so #1071's
/// replay-migrator can branch on [`SnapshotLoadError::FormatMismatch`].
#[derive(Debug)]
pub enum SnapshotLoadError {
    /// The snapshot's declared `format_version` is newer than this build can
    /// read. #1071 branches on this to migration; fail-closed is the fallback.
    /// (This is the `SnapshotFormatMismatch { found, supported }` of the epic.)
    FormatMismatch {
        /// The `format_version` found in the on-disk header.
        found: u32,
        /// The highest `format_version` this build supports
        /// ([`SNAPSHOT_FORMAT_VERSION`]).
        supported: u32,
    },
    /// The snapshot file is present but its header or payload could not be
    /// parsed. Fatal — never a silent `None`, never a rewinding fallback.
    Corrupt {
        /// Human-readable reason (which stage failed and why).
        reason: String,
    },
}

impl std::fmt::Display for SnapshotLoadError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            SnapshotLoadError::FormatMismatch { found, supported } => write!(
                f,
                "snapshot format version {found} is newer than this build supports \
                 (max {supported}); refusing to load (fail-closed)"
            ),
            SnapshotLoadError::Corrupt { reason } => {
                write!(f, "snapshot present but unreadable: {reason}")
            }
        }
    }
}

impl std::error::Error for SnapshotLoadError {}

impl From<SnapshotLoadError> for io::Error {
    fn from(e: SnapshotLoadError) -> io::Error {
        io::Error::new(io::ErrorKind::InvalidData, e)
    }
}

/// The coarse engine build identity stamped into [`SnapshotHeader::engine_fingerprint`].
fn engine_fingerprint() -> String {
    format!("nanobpmn-engine-core@{}", nanobpmn_engine_core::VERSION)
}

/// Reads (or, on first use, generates and durably persists) this data
/// directory's incarnation id. Stable for the lifetime of the data dir; a
/// reset/restore that removes [`INCARNATION_NAME`] yields a fresh id, which is
/// how nano-workforce#622 detects a reset/restore.
fn read_or_init_incarnation(dir: &Path) -> io::Result<u64> {
    let path = dir.join(INCARNATION_NAME);
    // Only a genuinely *missing* file seeds a fresh incarnation — that is the
    // reset/restore signal (nano-workforce#622). A transient read failure
    // (e.g. a permission error) or corrupt/zero contents must NOT be treated as
    // a reset: silently regenerating would spuriously change the incarnation and
    // contradict the stated "stable unless reset/restore" contract, surfacing a
    // false reset to the cross-repo consumer. Fail closed (#1066) in those cases.
    match fs::read_to_string(&path) {
        Ok(s) => {
            let v = s.trim().parse::<u64>().map_err(|e| {
                io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("incarnation file {path:?} has non-numeric contents: {e}"),
                )
            })?;
            if v == 0 {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!("incarnation file {path:?} contains reserved value 0"),
                ));
            }
            return Ok(v);
        }
        Err(e) if e.kind() == io::ErrorKind::NotFound => {
            // Fall through to seed a fresh incarnation — the reset/restore case.
        }
        Err(e) => return Err(e),
    }
    // Seed from wall-clock nanoseconds (monotonic enough to distinguish
    // successive incarnations of the same dir); persisted so it is stable.
    // Clamp to a non-zero value: `0` is reserved for the synthetic legacy
    // headerless envelope (`incarnation: 0` in `parse_envelope`) and the
    // cross-repo contract (nano-workforce#622) requires a non-zero id, so a
    // clock before UNIX_EPOCH (or a stale on-disk `0`) must not surface as `0`.
    let incarnation = std::time::SystemTime::now()
        .duration_since(std::time::UNIX_EPOCH)
        .map(|d| d.as_nanos() as u64)
        .unwrap_or(0)
        .max(1);
    let tmp = dir.join(format!("{INCARNATION_NAME}.tmp"));
    {
        let mut f = File::create(&tmp)?;
        f.write_all(incarnation.to_string().as_bytes())?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &path)?;
    fsync_dir(dir);
    Ok(incarnation)
}

/// Writes `payload` to `path` atomically (tmp + rename + fsync), prefixed with
/// the versioned envelope header. `dir` is the containing data dir (source of
/// the incarnation id). Shared by [`write_snapshot`] and [`write_multi_snapshot`].
fn write_enveloped<T: serde::Serialize>(dir: &Path, path: &Path, payload: &T) -> io::Result<()> {
    let header = SnapshotHeader {
        format_version: SNAPSHOT_FORMAT_VERSION,
        incarnation: read_or_init_incarnation(dir)?,
        engine_fingerprint: engine_fingerprint(),
    };
    let tmp = path.with_extension("bin.tmp");
    {
        // Stream to the file (BufWriter) rather than building a full `Vec<u8>`
        // first; under a large backlog the intermediate buffer is multi-GB (all
        // resident variable payloads serialized at once).
        let f = File::create(&tmp)?;
        let mut w = BufWriter::new(f);
        // Header first, on its own line — always parseable, fixed shape.
        serde_json::to_writer(&mut w, &header)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        w.write_all(b"\n")?;
        serde_json::to_writer(&mut w, payload)
            .map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let f = w.into_inner()?;
        f.sync_all()?;
    }
    fs::rename(&tmp, path)?;
    fsync_dir(dir);
    Ok(())
}

/// Splits raw snapshot bytes into `(header, payload)`. A missing header line
/// (no `\n`, i.e. legacy headerless compact JSON) yields the synthetic
/// `format_version = 0` header and the whole file as payload. A present-but-
/// unparseable header line is a fatal [`SnapshotLoadError::Corrupt`].
fn parse_envelope(bytes: &[u8]) -> Result<(SnapshotHeader, &[u8]), SnapshotLoadError> {
    match bytes.iter().position(|&b| b == b'\n') {
        Some(nl) => {
            let header: SnapshotHeader =
                serde_json::from_slice(&bytes[..nl]).map_err(|e| SnapshotLoadError::Corrupt {
                    reason: format!("unparseable envelope header: {e}"),
                })?;
            Ok((header, &bytes[nl + 1..]))
        }
        None => Ok((
            SnapshotHeader {
                format_version: 0,
                incarnation: 0,
                engine_fingerprint: String::new(),
            },
            bytes,
        )),
    }
}

/// Validates the header's `format_version` against what this build supports.
/// A version this build cannot read is a typed [`SnapshotLoadError::FormatMismatch`].
fn check_format_version(header: &SnapshotHeader) -> Result<(), SnapshotLoadError> {
    if header.format_version > SNAPSHOT_FORMAT_VERSION {
        return Err(SnapshotLoadError::FormatMismatch {
            found: header.format_version,
            supported: SNAPSHOT_FORMAT_VERSION,
        });
    }
    Ok(())
}

/// Reads ONLY the `format_version` from a snapshot file's envelope header,
/// without deserializing the (possibly corrupt or incompatible) payload body.
/// `Ok(None)` when the file does not exist. Used by #1071's migrator and by the
/// envelope tests to prove the version is legible even from an unreadable payload.
pub fn peek_snapshot_format_version(path: &Path) -> io::Result<Option<u32>> {
    let file = match File::open(path) {
        Ok(f) => f,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    // We only need the envelope header — the first line (`\n`-terminated) — not
    // the (possibly multi-GB) payload body, so read just that line instead of
    // slurping the whole snapshot into memory. The read is capped so a legacy
    // headerless snapshot (compact JSON, no `\n`) can't force an unbounded read
    // either: an enveloped header is tiny, so no newline within the cap means
    // this is a legacy file, which `parse_envelope` maps to `format_version 0`.
    const HEADER_SCAN_CAP: u64 = 64 * 1024;
    let mut reader = BufReader::new(file.take(HEADER_SCAN_CAP));
    let mut line = Vec::new();
    reader.read_until(b'\n', &mut line)?;
    let (header, _) = parse_envelope(&line)?;
    Ok(Some(header.format_version))
}

fn is_seg_file(name: &str) -> Option<u64> {
    let rest = name.strip_prefix(SEG_PREFIX)?.strip_suffix(SEG_SUFFIX)?;
    rest.parse::<u64>().ok()
}

fn is_snap_file(name: &str) -> Option<u64> {
    let rest = name.strip_prefix(SNAP_PREFIX)?.strip_suffix(SNAP_SUFFIX)?;
    rest.parse::<u64>().ok()
}

/// Lists sealed segment files in `dir`, ascending by start index, pairing each
/// with the absolute event count it contains (by reading it). The active
/// segment is excluded.
fn list_sealed(dir: &Path) -> io::Result<Vec<(u64, PathBuf)>> {
    let mut segs: Vec<(u64, PathBuf)> = Vec::new();
    if dir.exists() {
        for entry in fs::read_dir(dir)? {
            let entry = entry?;
            let name = entry.file_name();
            let name = name.to_string_lossy();
            if let Some(start) = is_seg_file(&name) {
                segs.push((start, entry.path()));
            }
        }
    }
    segs.sort_by_key(|(start, _)| *start);
    Ok(segs)
}

/// Index of the last line that carries content (any non-whitespace, non-NUL
/// byte), or `None` when every line is blank. Only this final line may be a torn
/// tail — a crash mid-append can leave an unterminated record or NUL padding at
/// end-of-file — so segment readers tolerate a parse failure *there* (dropping
/// it) while still hard-erroring on genuine mid-file corruption.
fn last_content_line(lines: &[&[u8]]) -> Option<usize> {
    lines
        .iter()
        .rposition(|l| l.iter().any(|&b| b != 0 && !b.is_ascii_whitespace()))
}

/// Reads and deserializes every event from a single segment/log file (empty if
/// absent).
pub fn read_segment_events(path: &Path) -> io::Result<Vec<Event>> {
    let mut events = Vec::new();
    if path.exists() {
        let decoded = decode_segment_bytes(path)?;
        let lines: Vec<&[u8]> = decoded.split(|&b| b == b'\n').collect();
        let last = last_content_line(&lines);
        for (idx, line) in lines.iter().enumerate() {
            let torn_tail = Some(idx) == last;
            let line = match std::str::from_utf8(line) {
                Ok(s) => s,
                Err(e) if torn_tail => {
                    tracing::warn!("dropping torn journal tail in {}: {e}", path.display());
                    break;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            };
            if line.trim().is_empty() {
                continue;
            }
            let event: Event = match decode_event_json(line) {
                Ok(ev) => ev,
                // A torn trailing write can leave a truncated/garbled last line;
                // that is `Malformed` and safe to drop as an unfsynced tail. An
                // `UnknownVariant` is a COMPLETE, well-formed record naming an
                // event this build cannot replay (a renamed/removed variant or a
                // newer/foreign journal) — never a torn write, so it is fatal
                // even as the last line (routed to fail-closed / #1071, #1065).
                Err(EventDecodeError::Malformed { detail }) if torn_tail => {
                    tracing::warn!("dropping torn journal tail in {}: {detail}", path.display());
                    break;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            };
            events.push(event);
        }
    }
    Ok(events)
}

/// Reads a segment/log file written by the shared multi-partition writer, where
/// each line is `<partition>\t<event-json>` — the GLOBAL partition that produced
/// the write (see [`crate::journal::Journal::persist`]). Returns each event with
/// its write-partition tag.
///
/// Tolerates a bare `<event-json>` line (no tab): it falls back to the event's
/// key partition, so a directory written by the pre-tag multi-partition format
/// still recovers with the same demux behaviour it had then.
fn read_segment_events_tagged(path: &Path, num_partitions: usize) -> io::Result<Vec<(u64, Event)>> {
    let mut events = Vec::new();
    if path.exists() {
        let decoded = decode_segment_bytes(path)?;
        let lines: Vec<&[u8]> = decoded.split(|&b| b == b'\n').collect();
        let last = last_content_line(&lines);
        for (idx, line) in lines.iter().enumerate() {
            let torn_tail = Some(idx) == last;
            let line = match std::str::from_utf8(line) {
                Ok(s) => s,
                Err(e) if torn_tail => {
                    tracing::warn!("dropping torn journal tail in {}: {e}", path.display());
                    break;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            };
            if line.trim().is_empty() {
                continue;
            }
            let (tag, json) = match line.split_once('\t') {
                Some((t, rest)) => (t.parse::<u64>().ok(), rest),
                None => (None, line),
            };
            let event: Event = match decode_event_json(json) {
                Ok(ev) => ev,
                // Same torn-tail vs. unknown-frame distinction as
                // `read_segment_events`: tolerate only a `Malformed` tail; an
                // `UnknownVariant` is a real cross-version frame, never dropped.
                Err(EventDecodeError::Malformed { detail }) if torn_tail => {
                    tracing::warn!("dropping torn journal tail in {}: {detail}", path.display());
                    break;
                }
                Err(e) => return Err(io::Error::new(io::ErrorKind::InvalidData, e)),
            };
            let tag = tag.unwrap_or_else(|| {
                (partition_of(event.max_key()) as usize).min(num_partitions.saturating_sub(1))
                    as u64
            });
            events.push((tag, event));
        }
    }
    Ok(events)
}

/// Loads the latest persisted snapshot in `dir`.
///
/// `Ok(None)` means ONLY "no snapshot file exists". A snapshot that is present
/// but at an incompatible format version, or present but corrupt/unreadable, is
/// a FATAL typed error ([`SnapshotLoadError`], surfaced as an [`io::Error`]) —
/// never a silent `None`, never a fallback that rewinds durable state.
///
/// The loaded value keeps `covered_events` as the SECOND tuple element to match
/// the callers in [`recover`].
fn load_latest_snapshot(dir: &Path) -> io::Result<Option<(EngineSnapshot, u64)>> {
    let mut best: Option<(u64, PathBuf)> = None;
    let rd = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    // A read_dir entry error would otherwise be silently skipped by `.flatten()`,
    // which could return `Ok(None)` (or pick a stale snapshot) despite a present
    // snapshot — undermining the fail-closed guarantee. Propagate it instead.
    for entry in rd {
        let entry = entry?;
        let name = entry.file_name();
        if let Some(covered) = is_snap_file(&name.to_string_lossy())
            && best.as_ref().map(|(c, _)| covered > *c).unwrap_or(true)
        {
            best = Some((covered, entry.path()));
        }
    }
    let Some((_, path)) = best else {
        return Ok(None);
    };
    // A file we selected by name is PRESENT: from here on, every failure is fatal.
    let bytes = fs::read(&path)?;
    let (header, payload) = parse_envelope(&bytes)?;
    check_format_version(&header)?;
    let snap: PersistedSnapshot =
        serde_json::from_slice(payload).map_err(|e| SnapshotLoadError::Corrupt {
            reason: format!("snapshot payload deserialize failed: {e}"),
        })?;
    Ok(Some((snap.engine, snap.covered_events)))
}

/// The outcome of recovering a segmented journal directory.
pub struct SegRecovery {
    /// `false` when prior durable state was recovered.
    pub fresh: bool,
    /// The shared seal/boundary state to hand to the writer.
    pub shared: Arc<SegShared>,
    /// All surviving events, ascending, spanning `[first_index, total_events)`.
    pub events: Vec<Event>,
    /// Absolute index of the first surviving event (events compacted before this
    /// are only present in the snapshot / read model).
    pub first_index: u64,
    /// Absolute count of all events ever durably appended.
    pub total_events: u64,
}

/// Recovers (or initialises) the segmented journal in `dir`: loads the latest
/// snapshot, reads the surviving sealed tail + active segment, rebuilds the
/// engine (snapshot + tail, or full replay when there is no snapshot), and
/// returns the restored engine plus the shared state the writer continues from.
pub fn recover(dir: &Path) -> io::Result<(Engine, SegRecovery)> {
    fs::create_dir_all(dir)?;
    let active_path = dir.join(ACTIVE_NAME);

    let sealed_files = list_sealed(dir)?;

    // Read every surviving segment in order, tracking absolute indices from each
    // segment's start (encoded in its filename). Gaps cannot occur: compaction
    // only ever removes a contiguous prefix of sealed segments.
    let first_index = sealed_files.first().map(|(s, _)| *s);

    let mut sealed: Vec<SealedSeg> = Vec::with_capacity(sealed_files.len());
    let mut events: Vec<Event> = Vec::new();
    let mut cursor = first_index.unwrap_or(0);
    for (start, path) in &sealed_files {
        let seg_events = read_segment_events(path)?;
        let end = start + seg_events.len() as u64;
        sealed.push(SealedSeg {
            start: *start,
            end,
            path: path.clone(),
            per_partition_end: Vec::new(),
        });
        events.extend(seg_events);
        cursor = end;
    }

    // The active segment begins where the last sealed segment ended; if there
    // are no sealed segments, fall back to the persisted head (covers the case
    // where every sealed segment was compacted away), else 0.
    let active_start = if sealed.is_empty() {
        read_head(&dir.join(HEAD_NAME)).unwrap_or(0)
    } else {
        cursor
    };
    let active_events = read_segment_events(&active_path)?;
    let total_events = active_start + active_events.len() as u64;
    let first_index = first_index.unwrap_or(active_start);
    events.extend(active_events);

    // Load the snapshot ONCE, then rebuild the engine. A present snapshot that is
    // at an incompatible format version — or whose payload no longer deserializes
    // under this build — is a typed `SnapshotLoadError`. Rather than fail-closed
    // outright (or, far worse, silently rewind), attempt the #1071 replay-migrator:
    // rebuild from a from-scratch replay of the FULL history (cold archive +
    // surviving tail) and rewrite a fresh NEW-format snapshot. Fail-closed only
    // when the history cannot fully reconstruct the engine (a pruned gap, or an
    // unreadable event frame — both snapshot AND journal gone).
    let (engine, fresh) = match load_latest_snapshot(dir) {
        // A readable snapshot whose `covered` is BELOW the compaction floor is
        // stale (e.g. the newest snapshot was lost/removed post-compaction and an
        // older one resurfaced). Trusting it would `saturating_sub`-clamp `skip`
        // to 0 and replay only the surviving tail atop an incomplete base, silently
        // dropping the compacted `[covered, first_index)` window and rewinding the
        // key generator (#1065). This is a distinct signal from an unreadable /
        // incompatible snapshot (which migrates by replay): a stale-but-valid
        // snapshot means the durable set is inconsistent, so fail closed loud.
        Ok(Some((_snap, covered))) if first_index > 0 && covered < first_index => {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "snapshot covers only up to {covered} but the journal is compacted (first \
                     surviving event index {first_index}); refusing to recover across the \
                     compacted gap [{covered}, {first_index}), which would rewind the engine key \
                     generator and drop compacted state. See issue #1065."
                ),
            ));
        }
        Ok(Some((snap, covered))) => {
            let mut engine = Engine::from_snapshot(snap);
            // Replay only events after the snapshot boundary.
            let skip = covered.saturating_sub(first_index) as usize;
            if skip < events.len() {
                engine.apply_replayed_events(events[skip..].iter().cloned());
            }
            (engine, false)
        }
        // No snapshot present. If the hot journal still starts at index 0 the
        // surviving events ARE the whole history — replay them cheaply. But if
        // compaction advanced `first_index` above 0, the tail alone is missing
        // `[0, first_index)`: replaying only it would silently rewind (#1065).
        // Route through the same cold-archive reconstruction / fail-closed gate
        // as an unreadable snapshot.
        Ok(None) if first_index == 0 => {
            let fresh = total_events == 0;
            (Engine::replay_partition(0, events.iter().cloned()), fresh)
        }
        Ok(None) => match migrate_by_replay(dir, 0, first_index, total_events, &events)? {
            Some(engine) => {
                tracing::warn!(
                    "no snapshot but the journal starts at {first_index}; reconstructed by \
                     replaying the cold archive + surviving tail and rewriting a fresh v{} \
                     snapshot",
                    SNAPSHOT_FORMAT_VERSION
                );
                if let Err(we) = write_snapshot(dir, engine.snapshot(), total_events) {
                    tracing::warn!("post-reconstruction snapshot write failed: {we}");
                } else {
                    prune_cold_archive(dir, total_events);
                }
                (engine, false)
            }
            // The cold archive cannot rebuild `[0, first_index)`: fail closed
            // (#1066) rather than replaying only the tail and rewinding (#1065).
            None => {
                return Err(io::Error::new(
                    io::ErrorKind::InvalidData,
                    format!(
                        "no snapshot and the cold archive cannot reconstruct \
                         [0, {first_index}); refusing to replay only the surviving tail \
                         (would rewind)"
                    ),
                ));
            }
        },
        Err(e) => {
            // A non-typed io error (a real read failure) just propagates.
            if e.get_ref()
                .and_then(|r| r.downcast_ref::<SnapshotLoadError>())
                .is_none()
            {
                return Err(e);
            }
            match migrate_by_replay(dir, 0, first_index, total_events, &events)? {
                Some(engine) => {
                    tracing::warn!(
                        "snapshot unreadable ({e}); migrated by replaying the journal \
                         (cold archive + surviving tail) and rewriting a fresh v{} snapshot",
                        SNAPSHOT_FORMAT_VERSION
                    );
                    // Persist the migrated engine as a fresh new-format snapshot
                    // covering the whole replayed history (this also deletes the
                    // superseded incompatible snapshot), then prune the cold
                    // archive up to it. Best-effort: a write failure just means the
                    // next boot migrates again.
                    if let Err(we) = write_snapshot(dir, engine.snapshot(), total_events) {
                        tracing::warn!("post-migration snapshot write failed: {we}");
                    } else {
                        prune_cold_archive(dir, total_events);
                    }
                    (engine, false)
                }
                // Replay cannot reconstruct without rewinding: fail-closed (#1066).
                None => return Err(e),
            }
        }
    };

    let shared = Arc::new(SegShared {
        dir: dir.to_path_buf(),
        active_path,
        total_events: AtomicU64::new(total_events),
        active_start: AtomicU64::new(active_start),
        sealed: Mutex::new(sealed),
        segment_bytes: segment_bytes_from_env(),
        per_partition_total: Vec::new(),
        per_partition_active_start: Vec::new(),
        compress: journal_compress_from_env(),
    });

    Ok((
        engine,
        SegRecovery {
            fresh,
            shared,
            events,
            first_index,
            total_events,
        },
    ))
}

/// Persists `snap` (covering `covered_events`) atomically to `dir`, then removes
/// any older snapshot files. Called by the maintenance task after rotating.
pub fn write_snapshot(dir: &Path, snap: EngineSnapshot, covered_events: u64) -> io::Result<()> {
    let payload = PersistedSnapshot {
        covered_events,
        engine: snap,
    };
    let final_path = dir.join(format!("{SNAP_PREFIX}{covered_events:020}{SNAP_SUFFIX}"));
    write_enveloped(dir, &final_path, &payload)?;

    // Drop superseded snapshots (keep only the newest covered count).
    if let Ok(rd) = fs::read_dir(dir) {
        for entry in rd.flatten() {
            let name = entry.file_name();
            if let Some(covered) = is_snap_file(&name.to_string_lossy())
                && covered < covered_events
            {
                let _ = fs::remove_file(entry.path());
            }
        }
    }
    Ok(())
}

/// Deletes every sealed segment fully covered by `watermark` — the lesser of the
/// latest snapshot's covered count and the read model's exported position — so a
/// segment is removed only once *both* the engine snapshot and the read model no
/// longer need it. The active segment is never touched. Returns the number of
/// segments removed.
pub fn compact(shared: &SegShared, watermark: u64) -> usize {
    let mut sealed = shared.sealed.lock().expect("sealed lock");
    let mut removed = 0usize;
    // Sealed segments are kept ascending; remove the covered prefix — but ARCHIVE
    // each covered segment into the bounded cold store first, so a later snapshot
    // format-version migration can still REPLAY it (see the cold-archive section
    // and [`recover`]'s migrator). A segment is only removed from the hot log once
    // its events are durably in the cold store; if archiving fails we KEEP the
    // segment (deletion never precedes a durable archive), so we can never lose
    // the covered prefix — the #1065 retention gap that let a snapshot-format
    // mismatch rewind durable state.
    while let Some(seg) = sealed.first() {
        if seg.end <= watermark {
            if let Err(e) = archive_cold_segment(&shared.dir, &seg.path, seg.start, seg.end) {
                tracing::warn!(
                    "cold-archiving sealed segment {} failed: {e}; keeping it (not deleting)",
                    seg.path.display()
                );
                break;
            }
            let _ = fs::remove_file(&seg.path);
            sealed.remove(0);
            removed += 1;
        } else {
            break;
        }
    }
    if removed > 0 {
        fsync_dir(&shared.dir);
    }
    removed
}

// ----------------------------------------------------------------------------
// Rolling cold journal archive (bounded) — L5 / #1071.
//
// The journal analog of the read-model's durable `terminal-archive.sqlite`
// (#831). Before L5, `compact`/`compact_multi` HARD-DELETED a sealed segment once
// it was covered by both the snapshot watermark and the read-model exported
// position — so once compacted there was nothing left to replay from. If the
// engine snapshot then turned out to be at an unreadable format version, boot had
// only the surviving (post-compaction) tail to replay, silently REWINDING every
// instance whose creation had been compacted away (incident #1065, "instance 41").
//
// The fix: at compaction, instead of deleting the covered prefix, ARCHIVE it —
// compressed — into a cold store (`journal.cold.<start>.<end>.jsonl.z`), so the
// full event history back to the archive floor is still replayable. The store is
// kept BOUNDED by pruning it one snapshot generation back after a fresh
// SAME-format snapshot is durably written ([`prune_cold_archive`]): the newest
// generation is retained as the migration fallback, older generations are already
// folded into the current (readable) snapshot and are dropped. Net cold-archive
// size ~= one snapshot generation of events — the same disk profile as before.
//
// A cold file stores the segment's DECODED plaintext records (inner journal
// frames already inflated by [`decode_segment_bytes`]), then deflated as a whole
// — so the single-partition (`<json>`) and multi-partition (`<partition>\t<json>`)
// line formats are preserved verbatim and re-read by the same decoders. Sealed
// segments are fsynced-before-rename, so a cold file is never torn: any decode
// failure on read is FATAL (routed to fail-closed), never tolerated as a tail.
// ----------------------------------------------------------------------------

/// Cold-archive filename prefix: `journal.cold.<start>.<end>.jsonl.z`.
const COLD_PREFIX: &str = "journal.cold.";
/// Cold-archive filename suffix (deflated plaintext records).
const COLD_SUFFIX: &str = ".jsonl.z";

/// The cold-archive path for the segment covering `[start, end)`.
fn cold_name(dir: &Path, start: u64, end: u64) -> PathBuf {
    dir.join(format!("{COLD_PREFIX}{start:020}.{end:020}{COLD_SUFFIX}"))
}

/// Parses a cold-archive filename into its `(start, end)` global event range.
fn is_cold_file(name: &str) -> Option<(u64, u64)> {
    let rest = name.strip_prefix(COLD_PREFIX)?.strip_suffix(COLD_SUFFIX)?;
    let (start, end) = rest.split_once('.')?;
    Some((start.parse::<u64>().ok()?, end.parse::<u64>().ok()?))
}

/// Deflates `bytes` (best-effort ratio; whole-buffer, not framed).
fn deflate_all(bytes: &[u8]) -> io::Result<Vec<u8>> {
    use flate2::{Compression, write::DeflateEncoder};
    let mut enc = DeflateEncoder::new(Vec::with_capacity(bytes.len() / 2), Compression::fast());
    enc.write_all(bytes)?;
    enc.finish()
}

/// Inflates a whole-buffer deflate stream written by [`deflate_all`].
fn inflate_all(bytes: &[u8]) -> io::Result<Vec<u8>> {
    use std::io::Read;

    use flate2::read::DeflateDecoder;
    let mut out = Vec::with_capacity(bytes.len().saturating_mul(2));
    DeflateDecoder::new(bytes).read_to_end(&mut out)?;
    Ok(out)
}

/// Archives the sealed segment at `seg_path` (covering `[start, end)`) into the
/// cold store: reads its DECODED plaintext records (inflating any inner frames),
/// deflates the whole thing, and writes it atomically (tmp + rename + fsync). The
/// caller deletes the hot segment only after this returns `Ok` — so the covered
/// prefix is never lost. Idempotent: an already-present cold file for the same
/// range is left as-is (a re-run of compaction over a kept segment).
fn archive_cold_segment(dir: &Path, seg_path: &Path, start: u64, end: u64) -> io::Result<()> {
    let final_path = cold_name(dir, start, end);
    if final_path.exists() {
        return Ok(());
    }
    let plaintext = decode_segment_bytes(seg_path)?;
    let compressed = deflate_all(&plaintext)?;
    let tmp = final_path.with_extension("z.tmp");
    {
        let mut f = File::create(&tmp)?;
        f.write_all(&compressed)?;
        f.sync_all()?;
    }
    fs::rename(&tmp, &final_path)?;
    fsync_dir(dir);
    Ok(())
}

/// Lists cold-archive files in `dir`, ascending by start index, as
/// `(start, end, path)`. Empty when there is no cold store.
fn list_cold(dir: &Path) -> io::Result<Vec<(u64, u64, PathBuf)>> {
    let mut cold: Vec<(u64, u64, PathBuf)> = Vec::new();
    let rd = match fs::read_dir(dir) {
        Ok(rd) => rd,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(cold),
        Err(e) => return Err(e),
    };
    for entry in rd {
        let entry = entry?;
        let name = entry.file_name();
        if let Some((start, end)) = is_cold_file(&name.to_string_lossy()) {
            cold.push((start, end, entry.path()));
        }
    }
    cold.sort_by_key(|(start, _, _)| *start);
    Ok(cold)
}

/// The global event bounds `[min_start, max_end)` the cold archive currently
/// spans, or `None` when it is empty. This is ONLY a bounds calculation over the
/// cold files' start/end indices — it does **not** assert the archive is a
/// gap-free contiguous prefix (an internal gap, overlap, or mis-sized file is not
/// detected here). The migrator uses it (together with the hot journal's
/// `first_index`) for a cheap coverage estimate, but the actual soundness gate is
/// [`read_cold_prefix`]'s validated `[0, cold_end)` walk.
pub fn cold_archive_span(dir: &Path) -> io::Result<Option<(u64, u64)>> {
    let cold = list_cold(dir)?;
    let Some((first_start, _, _)) = cold.first() else {
        return Ok(None);
    };
    let first_start = *first_start;
    let last_end = cold
        .iter()
        .map(|(_, end, _)| *end)
        .max()
        .unwrap_or(first_start);
    Ok(Some((first_start, last_end)))
}

/// Decodes ONE cold-archive file into its events (single-partition). A decode
/// failure is FATAL (the cold store is fully durable, never torn): an
/// [`EventDecodeError::UnknownVariant`] surfaces as a downcastable [`io::Error`]
/// so the migrator routes it to fail-closed (#1066) rather than reconstructing
/// from a partial history.
fn parse_cold_file(path: &Path) -> io::Result<Vec<Event>> {
    let plaintext = inflate_all(&fs::read(path)?)?;
    let mut events = Vec::new();
    for line in plaintext.split(|&b| b == b'\n') {
        let line =
            std::str::from_utf8(line).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if line.trim().is_empty() {
            continue;
        }
        let event =
            decode_event_json(line).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        events.push(event);
    }
    Ok(events)
}

/// Decodes ONE cold-archive file into `(write-tag, event)` pairs (multi-partition
/// — the cold analog of [`read_segment_events_tagged`]). A bare (untagged) line
/// falls back to the event's key partition, exactly as the hot tagged reader.
/// Decode failures are FATAL (never a torn tail).
fn parse_cold_file_tagged(path: &Path, num_partitions: usize) -> io::Result<Vec<(u64, Event)>> {
    let plaintext = inflate_all(&fs::read(path)?)?;
    let mut events = Vec::new();
    for line in plaintext.split(|&b| b == b'\n') {
        let line =
            std::str::from_utf8(line).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        if line.trim().is_empty() {
            continue;
        }
        let (tag, json) = match line.split_once('\t') {
            // A tab delimits the write-partition tag. Cold files are authoritative
            // and never torn (decode failures are FATAL here), and event JSON
            // never contains a literal tab (serde escapes it as `\t`), so a tab
            // present with a NON-numeric tag is corruption — fail closed rather
            // than silently treating it as untagged and misrouting to the key
            // partition. A line with no tab at all is a legitimately untagged
            // (pre-tag format) record and still falls back below.
            Some((t, rest)) => {
                let tag = t.parse::<u64>().map_err(|e| {
                    io::Error::new(
                        io::ErrorKind::InvalidData,
                        format!(
                            "cold archive file {} has a non-numeric write tag {t:?}: {e}",
                            path.display()
                        ),
                    )
                })?;
                (Some(tag), rest)
            }
            None => (None, line),
        };
        let event =
            decode_event_json(json).map_err(|e| io::Error::new(io::ErrorKind::InvalidData, e))?;
        let tag = tag.unwrap_or_else(|| {
            (partition_of(event.max_key()) as usize).min(num_partitions.saturating_sub(1)) as u64
        });
        events.push((tag, event));
    }
    Ok(events)
}

/// Reads the cold archive as a VALIDATED contiguous prefix starting at global
/// index 0, returning `(events, cold_end)` — the events of `[0, cold_end)`.
///
/// This is the migrator's safety gate: `cold_archive_span` alone only reports the
/// min-start / max-end and so cannot distinguish a genuine `[0, N)` prefix from a
/// set with an internal gap, an overlap, or a mis-sized file. Here we walk the
/// files in ascending order and accumulate ONLY while each abuts the previous
/// (`start == expected`) and holds EXACTLY `end - start` decoded records, so a
/// gap/overlap simply truncates `cold_end` (the caller then sees `cold_end !=
/// first_index` and fails closed rather than replaying a hole). A per-file
/// record-count mismatch — or an undecodable frame — is a hard `Err`
/// (fail-closed): the journal is not trustworthy. An empty (or non-zero-starting)
/// archive yields `(vec![], 0)`.
fn read_cold_prefix(dir: &Path) -> io::Result<(Vec<Event>, u64)> {
    let mut events = Vec::new();
    let mut expected = 0u64;
    for (start, end, path) in list_cold(dir)? {
        if start != expected {
            break; // gap or overlap: the contiguous prefix ends here
        }
        let decoded = parse_cold_file(&path)?;
        if decoded.len() as u64 != end.saturating_sub(start) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cold archive file {} holds {} records but its range [{start}, {end}) \
                     expects {}",
                    path.display(),
                    decoded.len(),
                    end.saturating_sub(start)
                ),
            ));
        }
        events.extend(decoded);
        expected = end;
    }
    Ok((events, expected))
}

/// [`read_cold_prefix`] with write-partition tags (multi-partition path).
fn read_cold_prefix_tagged(
    dir: &Path,
    num_partitions: usize,
) -> io::Result<(Vec<(u64, Event)>, u64)> {
    let mut events = Vec::new();
    let mut expected = 0u64;
    for (start, end, path) in list_cold(dir)? {
        if start != expected {
            break;
        }
        let decoded = parse_cold_file_tagged(&path, num_partitions)?;
        if decoded.len() as u64 != end.saturating_sub(start) {
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "cold archive file {} holds {} records but its range [{start}, {end}) \
                     expects {}",
                    path.display(),
                    decoded.len(),
                    end.saturating_sub(start)
                ),
            ));
        }
        events.extend(decoded);
        expected = end;
    }
    Ok((events, expected))
}

/// Prunes the rolling cold archive, removing every cold file fully below
/// `keep_from` (i.e. `end <= keep_from`) — the events a fresh SAME-format snapshot
/// at (or past) `keep_from` has made redundant for normal recovery. Callers pass
/// the PREVIOUS snapshot generation's boundary so the most-recent generation is
/// retained as the migration fallback (a rolling one-generation window). Returns
/// the number of cold files removed.
pub fn prune_cold_archive(dir: &Path, keep_from: u64) -> usize {
    let mut removed = 0usize;
    let cold = match list_cold(dir) {
        Ok(c) => c,
        Err(e) => {
            tracing::warn!(
                "prune_cold_archive: skipping prune, failed to list cold archive in {}: {e}",
                dir.display()
            );
            return 0;
        }
    };
    for (_, end, path) in cold {
        if end <= keep_from {
            if fs::remove_file(&path).is_ok() {
                removed += 1;
            }
        } else {
            // Ascending by start; once a file's end is past the floor, so are
            // all later ones (segments are contiguous and non-overlapping).
            break;
        }
    }
    if removed > 0 {
        fsync_dir(dir);
    }
    removed
}

/// The cold-archive prune floor for one multi-partition maintenance tick: the
/// minimum covered watermark across the partitions THIS node actually snapshotted
/// (its OWNED partitions), i.e. the boundary every owned partition has folded into
/// the fresh combined snapshot.
///
/// `owned_covered` must carry exactly one entry per partition snapshotted this
/// tick — NOT a global-width vector padded with 0 for unowned partitions. A
/// clustered node owns only a subset of the cluster's partitions
/// (`partition_id % num_nodes == node_id`), so a global-width `covered` has
/// 0-holes for the partitions it does not own; taking the min across those holes
/// pins the floor at 0 forever, `prune_cold_archive` then never prunes, and the
/// shared cold archive grows without the intended one-generation bound (#1076).
/// Deriving the floor from the owned watermarks alone keeps the window bounded on
/// a clustered node while staying identical to the global min on a single node
/// that owns every partition.
pub fn cold_prune_floor(owned_covered: &[u64]) -> u64 {
    owned_covered.iter().copied().min().unwrap_or(0)
}

/// Rebuilds a single-partition engine from a from-scratch replay of the FULL
/// event history — the cold archive's covered prefix + the surviving hot tail —
/// when the on-disk snapshot cannot be loaded at its format version (the migrator
/// half of #1071). `hot_events` are the surviving events the caller already read,
/// covering `[first_index, total_events)`.
///
/// Returns `Ok(Some(engine))` when the full history `[0, total_events)` is
/// available and replays cleanly. Returns `Ok(None)` — deferring to the caller's
/// fail-closed path — when the history has a gap below the cold-archive floor
/// (the compacted prefix was already pruned and lives only in the unreadable
/// snapshot): reconstructing from there would REWIND, so we refuse. A replay that
/// hits an unreadable event frame (unknown variant) propagates as `Err`
/// (fail-closed): both the snapshot AND the journal are unreadable.
fn migrate_by_replay(
    dir: &Path,
    partition_id: u64,
    first_index: u64,
    total_events: u64,
    hot_events: &[Event],
) -> io::Result<Option<Engine>> {
    // Assemble the full history. The cold archive holds the compacted prefix as a
    // VALIDATED contiguous `[0, cold_end)` (gaps/overlaps/mis-sized files truncate
    // `cold_end`); the hot tail holds `[first_index, total_events)`. A from-scratch
    // replay is only SOUND when the two together cover `[0, total_events)` with no
    // gap: `cold_end == first_index` (cold meets the hot tail — with no cold that
    // requires `first_index == 0`) AND the hot tail spans exactly the rest.
    let (cold_events, cold_end) = read_cold_prefix(dir)?;
    let hot_covers_tail = hot_events.len() as u64 == total_events.saturating_sub(first_index);
    let full_from_zero = cold_end == first_index && hot_covers_tail;
    if !full_from_zero {
        tracing::error!(
            "snapshot-format migration cannot reconstruct partition {partition_id}: validated \
             cold prefix [0, {cold_end}) + hot tail [{first_index}, {total_events}) does not \
             contiguously cover [0, {total_events}); refusing to replay (would rewind) — failing \
             closed"
        );
        return Ok(None);
    }

    // Replay cold prefix then hot tail, in global order, from an empty engine.
    let engine = Engine::replay_partition(
        partition_id,
        cold_events.into_iter().chain(hot_events.iter().cloned()),
    );
    Ok(Some(engine))
}

/// The multi-partition migrator's shared "can we soundly replay the FULL
/// history?" gate. A from-scratch replay is only SOUND when the VALIDATED
/// contiguous cold prefix `[0, cold_end)` + hot tail `[first_index,
/// total_events)` together cover `[0, total_events)` with no gap: `cold_end ==
/// first_index` (with no cold that requires `first_index == 0`) AND the hot tail
/// spans exactly the rest. Returns the cold prefix's tagged events when that
/// holds, or `Ok(None)` when it does not (a pruned gap) — which every caller
/// turns into a fail-closed boot rather than a rewind. A real read/decode error
/// while walking the cold archive propagates as `Err`.
fn full_cold_prefix_tagged(
    dir: &Path,
    num_partitions: usize,
    first_index: u64,
    total_events: u64,
    hot_len: usize,
) -> io::Result<Option<Vec<(u64, Event)>>> {
    let (cold, cold_end) = read_cold_prefix_tagged(dir, num_partitions)?;
    let hot_covers_tail = hot_len as u64 == total_events.saturating_sub(first_index);
    if cold_end != first_index || !hot_covers_tail {
        tracing::error!(
            "multi-partition history cannot reconstruct: validated cold prefix [0, {cold_end}) \
             + hot tail [{first_index}, {total_events}) does not contiguously cover \
             [0, {total_events}); refusing to replay (would rewind) — failing closed"
        );
        return Ok(None);
    }
    Ok(Some(cold))
}

// ----------------------------------------------------------------------------
// Multi-partition (shared-WAL) bounded-disk path.
//
// One shared log carries every partition's events, interleaved in commit order.
// Positions (segment start/end, first_index, total_events, exported_position)
// are GLOBAL event indices exactly as in the single-partition path, so the read
// model stays a single global prefix. Compaction adds a per-partition SNAPSHOT
// gate: a sealed segment is deletable only once every partition's snapshot
// covers its own events within that segment. Per-partition counts are keyed by
// GLOBAL partition id (a node owning a subset of partitions leaves the rest at
// zero), which unifies single-node multi-partition and clustered.
// ----------------------------------------------------------------------------

/// One partition's entry in the combined snapshot.
#[derive(serde::Serialize, serde::Deserialize)]
struct MultiSnapshotEntry {
    partition: u64,
    covered: u64,
    engine: EngineSnapshot,
}

#[derive(serde::Serialize, serde::Deserialize)]
struct MultiPersistedSnapshot {
    entries: Vec<MultiSnapshotEntry>,
}

/// Persists the combined per-partition snapshot atomically (tmp + rename +
/// fsync). `entries` is `(global_partition_id, covered_count, snapshot)` for
/// every owned partition. Overwrites the previous combined snapshot.
pub fn write_multi_snapshot(
    dir: &Path,
    entries: Vec<(u64, u64, EngineSnapshot)>,
) -> io::Result<()> {
    let payload = MultiPersistedSnapshot {
        entries: entries
            .into_iter()
            .map(|(partition, covered, engine)| MultiSnapshotEntry {
                partition,
                covered,
                engine,
            })
            .collect(),
    };
    let final_path = dir.join(MULTI_SNAP_NAME);
    // Stream the JSON straight to the file through a BufWriter instead of
    // materialising the whole snapshot into a `Vec<u8>` first: under a large
    // active backlog that intermediate buffer is multi-GB (all resident variable
    // payloads serialized at once) and was a major driver of the transient RSS
    // balloon during the 60s snapshot tick. The snapshots share the live
    // variables by Arc, so only this serialization ever duplicated them.
    write_enveloped(dir, &final_path, &payload)?;
    Ok(())
}

/// Loads the combined per-partition snapshot, as a map from global partition id
/// to `(covered_count, snapshot)`.
///
/// `Ok(None)` means ONLY "no combined snapshot file exists". A present-but-
/// incompatible or corrupt file is a FATAL typed error ([`SnapshotLoadError`],
/// surfaced as an [`io::Error`]) — never a silent `None`, never a rewind.
fn load_multi_snapshot(
    dir: &Path,
) -> io::Result<Option<std::collections::HashMap<u64, (u64, EngineSnapshot)>>> {
    let bytes = match fs::read(dir.join(MULTI_SNAP_NAME)) {
        Ok(b) => b,
        Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
        Err(e) => return Err(e),
    };
    let (header, payload) = parse_envelope(&bytes)?;
    check_format_version(&header)?;
    let snap: MultiPersistedSnapshot =
        serde_json::from_slice(payload).map_err(|e| SnapshotLoadError::Corrupt {
            reason: format!("multi-snapshot payload deserialize failed: {e}"),
        })?;
    Ok(Some(
        snap.entries
            .into_iter()
            .map(|e| (e.partition, (e.covered, e.engine)))
            .collect(),
    ))
}

/// The outcome of recovering a multi-partition segmented journal directory.
pub struct MultiSegRecovery {
    /// `false` when prior durable state was recovered.
    pub fresh: bool,
    /// The shared seal/boundary state to hand to the shared writer.
    pub shared: Arc<SegShared>,
    /// All surviving events, ascending, spanning global `[first_index, total_events)`.
    pub events: Vec<Event>,
    /// Surviving events paired with the GLOBAL partition that produced each write
    /// (the write tag), in global log order. Drives the sharded read model's
    /// per-partition boot catch-up: shard `p` resumes from the events tagged `p`
    /// past its own persisted `exported_position` (see [`Self::pp_base`]).
    pub tagged: Vec<(u64, Event)>,
    /// Per-partition cumulative event counts BEFORE the first surviving event
    /// (i.e. the counts compacted away), indexed by global partition id. A shard's
    /// persisted `exported_position` minus `pp_base[p]` is how many surviving
    /// tagged events it has already projected.
    pub pp_base: Vec<u64>,
    /// Absolute (global) index of the first surviving event.
    pub first_index: u64,
    /// Absolute (global) count of all events ever durably appended.
    pub total_events: u64,
    /// Rebuilt engine per OWNED partition, keyed by global partition id.
    pub engines: Vec<(u64, Engine)>,
}

/// Recovers (or initialises) the multi-partition segmented journal in `dir` for
/// the partitions in `owned` (global ids). `num_partitions` is the global
/// partition count (sizes the per-partition vectors). Rebuilds each owned
/// partition's engine from the combined snapshot + its surviving tail (or a full
/// replay when there is no snapshot), and returns the shared state plus the
/// surviving events for the caller's global read-model catch-up.
pub fn recover_multi(
    dir: &Path,
    owned: &[u64],
    num_partitions: usize,
    varstore: Option<&crate::varstore::VarStore>,
) -> io::Result<MultiSegRecovery> {
    fs::create_dir_all(dir)?;
    let active_path = dir.join(ACTIVE_NAME);
    let sealed_files = list_sealed(dir)?;

    // `base_p`: per-partition cumulative counts BEFORE the first surviving event
    // (i.e. the count compacted away). For the first surviving sealed segment it
    // is its sidecar; with no sealed segments it is the per-partition head; with
    // nothing compacted it is zero.
    let first_sealed_start = sealed_files.first().map(|(s, _)| *s);
    let zeros = || vec![0u64; num_partitions];
    let pp_base: Vec<u64> = match first_sealed_start {
        Some(start) => read_pp_meta(&dir.join(format!(
            "{SEG_PREFIX}{start:020}{SEG_SUFFIX}{PP_META_SUFFIX}"
        )))
        .filter(|v| v.len() == num_partitions)
        .unwrap_or_else(zeros),
        None => read_pphead(&dir.join(PPHEAD_NAME))
            .filter(|v| v.len() == num_partitions)
            .unwrap_or_else(zeros),
    };

    // Read every surviving sealed segment in order, tracking global indices and
    // per-partition cumulative counts (keyed by each write's GLOBAL partition
    // TAG, from `pp_base`). Demultiplexing by the persisted write tag — not the
    // event's key partition — is what lets a clustered node route its durable
    // replicated `ProcessDeployed` (written under its first-owned partition but
    // keyed to the deployment partition) back to the partition that produced it.
    let first_index_opt = sealed_files.first().map(|(s, _)| *s);
    let mut sealed: Vec<SealedSeg> = Vec::with_capacity(sealed_files.len());
    // Surviving events with their write-partition tag, in global log order.
    let mut tagged: Vec<(u64, Event)> = Vec::new();
    let mut cursor = first_index_opt.unwrap_or(0);
    let mut pp_running = pp_base.clone();
    for (start, path) in &sealed_files {
        let seg_events = read_segment_events_tagged(path, num_partitions)?;
        let end = start + seg_events.len() as u64;
        for (tag, _) in &seg_events {
            let p = (*tag as usize).min(num_partitions.saturating_sub(1));
            if p < pp_running.len() {
                pp_running[p] += 1;
            }
        }
        sealed.push(SealedSeg {
            start: *start,
            end,
            path: path.clone(),
            per_partition_end: pp_running.clone(),
        });
        tagged.extend(seg_events);
        cursor = end;
    }

    // The active segment begins where the last sealed segment ended; with none,
    // fall back to the persisted head (every sealed segment compacted), else 0.
    let active_start = if sealed.is_empty() {
        read_head(&dir.join(HEAD_NAME)).unwrap_or(0)
    } else {
        cursor
    };
    // Per-partition active start == last sealed end (== pp_running), or the
    // per-partition head when no sealed segments survive.
    let per_partition_active_start: Vec<u64> = if sealed.is_empty() {
        pp_base.clone()
    } else {
        pp_running.clone()
    };

    let active_events = read_segment_events_tagged(&active_path, num_partitions)?;
    let total_events = active_start + active_events.len() as u64;
    let first_index = first_index_opt.unwrap_or(active_start);
    // Per-partition totals = active start + active-segment per-partition counts.
    let mut per_partition_total = per_partition_active_start.clone();
    for (tag, _) in &active_events {
        let p = (*tag as usize).min(num_partitions.saturating_sub(1));
        if p < per_partition_total.len() {
            per_partition_total[p] += 1;
        }
    }
    tagged.extend(active_events);

    // `events` (untagged, global order) drives the caller's read-model catch-up.
    let events: Vec<Event> = tagged.iter().map(|(_, e)| e.clone()).collect();

    // Replicated `ProcessDeployed` broadcast set: a durable deployment copy whose
    // write tag differs from its key partition (a clustered peer journaled it
    // under its first-owned partition, keyed to the deployment partition). It is
    // partition-agnostic, so every owned partition OTHER than the one that
    // produced it needs it installed. Applied version-guarded on recovery so an
    // older surviving copy never regresses a newer snapshot-held definition. In
    // single-node mode a deployment's tag equals its key partition, so this set
    // is empty and nothing is broadcast.
    let broadcast: Vec<(u64, Event)> = tagged
        .iter()
        .filter(|(tag, e)| {
            matches!(e, Event::ProcessDeployed { .. })
                && *tag
                    != (partition_of(e.max_key()) as usize).min(num_partitions.saturating_sub(1))
                        as u64
        })
        .cloned()
        .collect();

    // Load the combined snapshot, or fall into the #1071 replay-migrator when it
    // is present but unreadable (incompatible format version, or a payload that no
    // longer deserializes). Migration rebuilds EVERY owned partition from a
    // from-scratch replay of the full history (cold archive + surviving tail),
    // demuxed by write tag — never a rewind — and fail-closes when the history
    // cannot fully reconstruct (a pruned gap, or an unreadable event frame).
    let mut combined = None;
    // The full tagged history for a migration replay (cold-archived prefix +
    // surviving tail), populated only when we fall into the migrator below.
    let mut cold_tagged: Vec<(u64, Event)> = Vec::new();
    let migrate = match load_multi_snapshot(dir) {
        Ok(Some(c)) => {
            combined = Some(c);
            false
        }
        // No combined snapshot. If the journal still starts at index 0 the
        // surviving tail IS the whole history. But if compaction advanced
        // `first_index` above 0, replaying only each partition's surviving tail
        // would silently drop `[0, first_index)` and rewind (#1065): reconstruct
        // from the cold archive + tail through the same fail-closed gate as an
        // unreadable snapshot.
        Ok(None) if first_index == 0 => false,
        Ok(None) => {
            match full_cold_prefix_tagged(
                dir,
                num_partitions,
                first_index,
                total_events,
                tagged.len(),
            )? {
                Some(cold) => {
                    cold_tagged = cold;
                    tracing::warn!(
                        "no multi snapshot but the journal starts at {first_index}; \
                         reconstructing by replaying the cold archive + surviving tail and \
                         writing a fresh v{} snapshot",
                        SNAPSHOT_FORMAT_VERSION
                    );
                    true
                }
                // A pruned gap: fail closed rather than replaying only the tail
                // and rewinding (#1065 / #1066).
                None => {
                    return Err(io::Error::new(
                        io::ErrorKind::InvalidData,
                        "no multi snapshot and the cold archive cannot reconstruct \
                         [0, first_index): refusing to replay only the surviving tail \
                         (would rewind)",
                    ));
                }
            }
        }
        Err(e) => {
            if e.get_ref()
                .and_then(|r| r.downcast_ref::<SnapshotLoadError>())
                .is_none()
            {
                return Err(e);
            }
            match full_cold_prefix_tagged(
                dir,
                num_partitions,
                first_index,
                total_events,
                tagged.len(),
            )? {
                Some(cold) => {
                    cold_tagged = cold;
                    tracing::warn!(
                        "multi snapshot unreadable ({e}); migrating by replaying the journal \
                         (cold archive + surviving tail) and rewriting a fresh v{} snapshot",
                        SNAPSHOT_FORMAT_VERSION
                    );
                    true
                }
                // A pruned gap (or unreadable frame): fail closed with the
                // original typed snapshot error.
                None => return Err(e),
            }
        }
    };

    // When migrating, the replicated-deployment broadcast set must also consider
    // the cold-archived prefix (a cross-partition `ProcessDeployed` may have been
    // compacted out of the hot tail).
    let broadcast: Vec<(u64, Event)> = if migrate {
        cold_tagged
            .iter()
            .chain(tagged.iter())
            .filter(|(tag, e)| {
                matches!(e, Event::ProcessDeployed { .. })
                    && *tag
                        != (partition_of(e.max_key()) as usize)
                            .min(num_partitions.saturating_sub(1)) as u64
            })
            .cloned()
            .collect()
    } else {
        broadcast
    };

    let fresh = total_events == 0 && combined.is_none() && !migrate;

    // A compacted journal (`first_index > 0`) has had its event prefix deleted;
    // with no loadable multi-snapshot, replaying just the surviving tail would
    // rewind the partition key generators and silently drop compacted state —
    // refuse instead (see issue #1065). Skipped while `migrate` is set: migration
    // legitimately runs with `combined == None`, reconstructing the compacted
    // prefix from the cold archive (already validated by `full_cold_prefix_tagged`
    // to cover `[0, first_index)`), so it is not a truncated-tail replay.
    if !migrate && combined.is_none() && first_index > 0 {
        return Err(io::Error::new(
            io::ErrorKind::InvalidData,
            format!(
                "multi-partition journal is compacted (first surviving event index \
                 {first_index}) but no loadable snapshot is present; refusing to recover from \
                 a truncated tail, which would rewind the partition key generators and drop \
                 compacted state. See issue #1065."
            ),
        ));
    }

    // Per-partition compaction invariant, enforced defensively on the read path:
    // a partition whose prefix was compacted (`pp_base[p] > 0`) can only be
    // rebuilt if its snapshot entry actually covers the compaction floor
    // (`covered >= pp_base[p]`). A *missing* entry (the `None` arm would do a
    // truncated full replay) or a *stale* one (`covered < pp_base[p]`, whose
    // `saturating_sub` clamps `skip` to 0) would replay only that shard's
    // surviving tail across the compacted `[covered, pp_base[p])` gap — rewinding
    // its key generator and dropping compacted state, the same defect class as
    // #1065. Refuse loud instead. (This mirrors the
    // compaction rule "a segment is deletable only once every partition's
    // snapshot covers `per_partition_end[p]`".) Skipped while `migrate` is set:
    // migration rebuilds every owned partition from its FULL tagged history (cold
    // prefix + surviving tail), so no partition is replayed across a compacted gap.
    for &p in owned {
        if migrate {
            break;
        }
        let base_p = pp_base.get(p as usize).copied().unwrap_or(0);
        if base_p == 0 {
            continue;
        }
        let covered = combined.as_ref().and_then(|m| m.get(&p)).map(|(c, _)| *c);
        if covered.is_none_or(|c| c < base_p) {
            let detail = match covered {
                Some(c) => format!("its loadable snapshot entry only covers up to {c}"),
                None => "no loadable snapshot entry is present for it".to_string(),
            };
            return Err(io::Error::new(
                io::ErrorKind::InvalidData,
                format!(
                    "multi-partition journal partition {p} is compacted (first surviving index \
                     {base_p}) but {detail}; refusing to recover from a truncated tail, which \
                     would rewind that partition's key generator and drop compacted state. See \
                     issue #1065."
                ),
            ));
        }
    }

    // Rebuild each owned partition's engine: snapshot + its surviving tail, or a
    // full replay of its surviving events when there is no snapshot for it. Demux
    // by the write TAG (not the key partition). Then install any replicated
    // deployment broadcast produced under a DIFFERENT partition.
    //
    // Lean-snapshot recovery: when a `varstore` is supplied the combined snapshot
    // is control-only (no variable payloads), so the authoritative durable store
    // holds every live instance's variables as of the snapshot boundary. Load
    // them once and install them onto each from-snapshot engine *before* replaying
    // its tail, so the tail's variable merges/creates apply on the correct base
    // (installing after the tail would regress instances the tail updated). The
    // full-replay branch needs no install — it reconstructs variables from the
    // surviving `ProcessInstanceCreated`/`VariablesUpdated` events directly.
    //
    // Migration branch: rebuild the partition from a from-scratch replay of its
    // FULL tagged history (cold prefix + surviving tail), ignoring the unreadable
    // snapshot — same reconstruction as an unsnapshotted partition, but over the
    // complete history rather than only the tail.
    let stored_vars = varstore.map(|vs| vs.load_all());
    let engines: Vec<(u64, Engine)> = owned
        .iter()
        .map(|&p| {
            let p_events: Vec<Event> = tagged
                .iter()
                .filter(|(tag, _)| *tag == p)
                .map(|(_, e)| e.clone())
                .collect();
            let base_p = pp_base.get(p as usize).copied().unwrap_or(0);
            let mut engine = if migrate {
                let p_full: Vec<Event> = cold_tagged
                    .iter()
                    .chain(tagged.iter())
                    .filter(|(tag, _)| *tag == p)
                    .map(|(_, e)| e.clone())
                    .collect();
                Engine::replay_partition(p, p_full)
            } else {
                match combined.as_ref().and_then(|m| m.get(&p)) {
                    Some((covered, snap)) => {
                        let mut engine = Engine::from_snapshot(snap.clone());
                        if let Some(all) = stored_vars.as_ref() {
                            for (key, vars) in all {
                                if partition_of(*key) == p {
                                    engine.install_variables(*key, vars.clone());
                                }
                            }
                        }
                        let skip = covered.saturating_sub(base_p) as usize;
                        if skip < p_events.len() {
                            engine.apply_replayed_events(p_events[skip..].iter().cloned());
                        }
                        engine
                    }
                    None => Engine::replay_partition(p, p_events),
                }
            };
            // Install partition-agnostic replicated deployments produced under
            // another partition (skips those this partition already replayed as
            // its own tagged events). Version-guarded so it is monotonic.
            let broadcast_for_p: Vec<Event> = broadcast
                .iter()
                .filter(|(tag, _)| *tag != p)
                .map(|(_, e)| e.clone())
                .collect();
            if !broadcast_for_p.is_empty() {
                engine.install_deployment_if_newer(&broadcast_for_p);
            }
            (p, engine)
        })
        .collect();

    // Persist the migrated engines as a fresh new-format combined snapshot (each
    // partition covering its full per-partition history), then prune the cold
    // archive up to it. Best-effort: a write failure just means the next boot
    // migrates again.
    //
    // Only in the NON-lean path. A lean (var-store-backed) deployment keeps a
    // control-only snapshot in lockstep with the authoritative var store's
    // position: recovery does `from_snapshot` + `install_variables(store)` +
    // replay-tail. Writing a full/self-contained migration snapshot at
    // `total_events` here would leave the store lagging behind it, so the next
    // boot's `install_variables` would overwrite the freshly-migrated variables
    // with the store's older map (a rewind of exactly the kind #1065 is about).
    // Rather than reach into the engine to also rewrite the store, we leave the
    // incompatible snapshot in place (this boot already returns the correctly
    // migrated engines) and let the normal maintenance loop write the next
    // in-format lean snapshot + prune — idempotent, and the cold archive stays
    // bounded via that same loop. The migration simply re-runs on any crash-boot
    // before then, which is safe because we did not prune.
    if migrate && varstore.is_none() {
        let entries: Vec<(u64, u64, EngineSnapshot)> = engines
            .iter()
            .map(|(p, eng)| {
                let covered = per_partition_total.get(*p as usize).copied().unwrap_or(0);
                (*p, covered, eng.snapshot())
            })
            .collect();
        if let Err(we) = write_multi_snapshot(dir, entries) {
            tracing::warn!("post-migration multi snapshot write failed: {we}");
        } else {
            prune_cold_archive(dir, total_events);
        }
    }

    let shared = Arc::new(SegShared {
        dir: dir.to_path_buf(),
        active_path,
        total_events: AtomicU64::new(total_events),
        active_start: AtomicU64::new(active_start),
        sealed: Mutex::new(sealed),
        segment_bytes: segment_bytes_from_env(),
        per_partition_total: per_partition_total
            .into_iter()
            .map(AtomicU64::new)
            .collect(),
        per_partition_active_start: per_partition_active_start
            .into_iter()
            .map(AtomicU64::new)
            .collect(),
        compress: journal_compress_from_env(),
    });

    Ok(MultiSegRecovery {
        fresh,
        shared,
        events,
        tagged,
        pp_base,
        first_index,
        total_events,
        engines,
    })
}

/// Deletes every sealed segment that BOTH the read model and every partition's
/// snapshot no longer need: for every partition `p`, `exported[p] >=
/// seg.per_partition_end[p]` (that partition's read-model shard has projected all
/// of the segment's events — each shard consumes only its partition's events, in
/// log order) AND `covered[p] >= seg.per_partition_end[p]` (each partition's
/// snapshot subsumes its events in the segment). Both `exported` and `covered`
/// are indexed by global partition id (0 for partitions that never advanced).
///
/// The per-partition export gate replaces the earlier single global
/// `exported_position` scalar: with a sharded read model (one exporter thread +
/// store per partition) the shards advance independently, so a global sum could
/// pass while a lagging shard still needs a segment's events — deleting it would
/// lose data on that shard's boot re-projection.
///
/// Removes each segment's per-partition sidecar with it. Returns the count removed.
pub fn compact_multi(shared: &SegShared, covered: &[u64], exported: &[u64]) -> usize {
    let mut sealed = shared.sealed.lock().expect("sealed lock");
    let mut removed = 0usize;
    while let Some(seg) = sealed.first() {
        let export_ok = seg
            .per_partition_end
            .iter()
            .enumerate()
            .all(|(p, end)| exported.get(p).copied().unwrap_or(0) >= *end);
        let snap_ok = seg.per_partition_end.len() <= covered.len()
            && seg
                .per_partition_end
                .iter()
                .enumerate()
                .all(|(p, end)| covered.get(p).copied().unwrap_or(0) >= *end);
        if export_ok && snap_ok {
            // Archive the covered prefix into the cold store instead of hard-
            // deleting it (same rolling-window retention as the single-partition
            // `compact`), so a snapshot-format migration can still replay it. The
            // segment is removed only after its events are durably in the cold
            // store; if archiving fails we KEEP it rather than lose it.
            if let Err(e) = archive_cold_segment(&shared.dir, &seg.path, seg.start, seg.end) {
                tracing::warn!(
                    "cold-archiving sealed segment {} failed: {e}; keeping it (not deleting)",
                    seg.path.display()
                );
                break;
            }
            let _ = fs::remove_file(&seg.path);
            let _ = fs::remove_file(shared.pp_meta_path(seg.start));
            sealed.remove(0);
            removed += 1;
        } else {
            break;
        }
    }
    if removed > 0 {
        fsync_dir(&shared.dir);
    }
    removed
}

/// How a boot read-model catch-up must treat one shard, given its persisted
/// `exported_position` relative to the surviving journal window
/// `[floor, floor + surviving)`. `floor` is the shard's compaction boundary:
/// `SegRecovery::first_index` for the single-partition path, `pp_base[p]` for a
/// per-partition shard. Everything below `floor` has been compacted out of the
/// journal (folded into the engine snapshot) and no longer exists to replay.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum CatchUpPlan {
    /// The store sits inside the surviving window: resume projection by skipping
    /// the `skip` surviving events it has already projected, then project the rest.
    Resume { skip: usize },
    /// The store is *ahead* of the surviving log (a truncated/corrupt journal):
    /// reset it and rebuild from whatever survives.
    RebuildFromSurviving,
    /// The store sits *below* the compaction floor: the `missing` events in
    /// `[exported, floor)` it still needs were compacted out of the journal and
    /// cannot be replayed. Resuming would silently drop them — this is the
    /// data-loss failure mode of issue #600 and must never be handled silently.
    CompactedGap { missing: u64 },
}

/// Classifies a read-model shard's catch-up situation. Pure and total so the
/// three boot catch-up sites share one canonical decision (no drift), and the
/// compaction-gap detection is unit-testable in isolation.
///
/// In normal operation `exported >= floor` always holds — compaction is gated on
/// the exporter watermark, so the journal never discards events the read model
/// has not projected. `exported < floor` therefore only arises when the read
/// model was independently wiped or reset (a schema-fingerprint change across a
/// binary upgrade, an unreadable `read-model.sqlite`, or [`ReadStore::reset`])
/// while the journal had already been compacted.
pub fn plan_catch_up(exported: u64, floor: u64, surviving: u64) -> CatchUpPlan {
    if exported < floor {
        CatchUpPlan::CompactedGap {
            missing: floor - exported,
        }
    } else if exported > floor.saturating_add(surviving) {
        CatchUpPlan::RebuildFromSurviving
    } else {
        CatchUpPlan::Resume {
            skip: (exported - floor) as usize,
        }
    }
}

/// Catches one read-model shard up from a segmented recovery, using the single
/// canonical [`plan_catch_up`] decision. `surviving` are this shard's surviving
/// events (log-ordered, spanning `[floor, floor + surviving.len())`).
///
/// `reseed_state` is this shard's authoritative engine [`State`] as rebuilt for
/// boot (snapshot + surviving tail). When a [`CatchUpPlan::CompactedGap`] is hit —
/// the read model has been wiped/reset (e.g. a schema-fingerprint change across a
/// binary upgrade, or an unreadable `read-model.sqlite`) below the journal
/// compaction floor, so the events that would replay into it are gone — the shard
/// is REPROJECTED from that engine state instead of aborting (issue #732). The
/// compaction invariant guarantees the snapshot covers everything below the floor,
/// so every operationally-live entity is recovered losslessly; only terminal audit
/// history the engine already evicted (which only ever lived in the read model) is
/// not restored. This is logged loudly.
///
/// If no engine state is available (`reseed_state` is `None`), the historical
/// behaviour applies: abort with a panic, unless `NANOBPMN_READ_MODEL_LOSSY_REBUILD=1`
/// opts into a lossy rebuild from the (possibly empty) surviving tail — see #600.
pub fn catch_up_shard(
    shard: &crate::readstore::ReadStore,
    floor: u64,
    surviving: &[&Event],
    reseed_state: Option<&nanobpmn_engine_core::State>,
) {
    let exported = shard.exported_position() as u64;
    match plan_catch_up(exported, floor, surviving.len() as u64) {
        CatchUpPlan::Resume { skip } => {
            // A store migrated from before `event_waits` (schema v9) has no rows
            // for waits armed before the upgrade: backfill them from the engine
            // snapshot BEFORE replaying the tail it already reflects (a replayed
            // `*Created` is then a no-op and a replayed settle deletes the row).
            if let Some(state) = reseed_state
                && shard
                    .backfill_pending_event_waits(state)
                    .expect("backfill event waits from engine snapshot")
            {
                tracing::info!(
                    "backfilled open timer/signal/conditional waits from the engine snapshot \
                     after the read-model schema upgrade"
                );
            }
            // A store migrated across the job replay-floor boundary (schema v12)
            // backfilled `last_event_identity_ms` from `created_at_ms` alone —
            // below the genuine activation instant of a job that was activated and
            // returned to `Created` before the upgrade. Refine it from the engine
            // snapshot's `Job::activated_at` BEFORE replaying the tail it already
            // reflects, so a replayed `JobActivated` is held and a trailing
            // replayed `JobLockExpired` stays a no-op (#1346).
            if let Some(state) = reseed_state
                && shard
                    .refine_job_replay_floor_from_state(state)
                    .expect("refine job replay floor from engine snapshot")
            {
                tracing::info!(
                    "refined the job replay-identity floor from the engine snapshot \
                     after the read-model schema upgrade"
                );
            }
            if skip < surviving.len() {
                shard
                    .export(&surviving[skip..])
                    .expect("catch up read model from segmented journal");
            }
        }
        CatchUpPlan::RebuildFromSurviving => {
            rebuild_from_surviving_tail(
                shard,
                floor,
                surviving,
                "rebuild read model from surviving journal tail",
            );
        }
        CatchUpPlan::CompactedGap { missing } => {
            if let Some(state) = reseed_state {
                let total = floor.saturating_add(surviving.len() as u64);
                tracing::warn!(
                    exported,
                    floor,
                    missing,
                    total,
                    "read model sits below the journal compaction floor (wiped or reset while \
                     the journal was compacted): {missing} events were compacted out of the \
                     journal and cannot be replayed. Reprojecting the read model from the \
                     authoritative engine snapshot instead (issue #732) — all live process \
                     instances, jobs, incidents, user tasks, variables, subscriptions and \
                     definitions are recovered. Terminal/completed audit history that predates \
                     this boot is then restored from the durable terminal-audit archive where \
                     available (issue #831); any history never written to the archive (e.g. \
                     completed before the archive existed) is NOT restored."
                );
                reseed_from_engine_state(shard, total, state);
            } else if read_model_lossy_rebuild_enabled() {
                tracing::error!(
                    exported,
                    floor,
                    missing,
                    "read model sits below the journal compaction floor (wiped or reset while \
                     the journal was compacted): {missing} events were compacted out of the \
                     journal and cannot be replayed. No engine snapshot is available to \
                     reproject from and NANOBPMN_READ_MODEL_LOSSY_REBUILD is set — rebuilding \
                     from the surviving journal tail and PERMANENTLY DROPPING the compacted \
                     history."
                );
                rebuild_from_surviving_tail(
                    shard,
                    floor,
                    surviving,
                    "lossy rebuild read model from surviving journal tail",
                );
            } else {
                panic!(
                    "read model at exported_position={exported} is below the journal compaction \
                     floor={floor}: {missing} events were compacted out of the journal (folded \
                     into the engine snapshot) and can no longer be replayed to rebuild the read \
                     model, and no engine snapshot was available to reproject from. This happens \
                     when the read model is wiped or reset — e.g. a schema-fingerprint change \
                     across a binary upgrade, or an unreadable read-model.sqlite — while the \
                     segmented journal has already been compacted. Proceeding would SILENTLY lose \
                     every pre-compaction process instance. Restore the read model \
                     (read-model.sqlite) from a backup, or set NANOBPMN_READ_MODEL_LOSSY_REBUILD=1 \
                     to rebuild from the surviving journal tail and accept the loss. See issues \
                     #600 and #732."
                );
            }
        }
    }
}

/// Resets a shard and reprojects it from the authoritative boot engine `state`,
/// then plants the absolute cursor at `total` (== `floor + surviving.len()`, the
/// full projected event count this state already reflects). Setting the cursor is
/// essential: `exported_position` is an ABSOLUTE event index checked against the
/// compaction floor on every boot, so leaving it below the floor would re-trip
/// [`CatchUpPlan::CompactedGap`] on the next boot and reproject forever.
fn reseed_from_engine_state(
    shard: &crate::readstore::ReadStore,
    total: u64,
    state: &nanobpmn_engine_core::State,
) {
    shard.reset().expect("reset read store");
    shard
        .seed_from_engine_state(state)
        .expect("reproject read model from engine snapshot");
    // Replay the durable terminal-audit archive on top of the live snapshot
    // (issue #831): the snapshot only carries live instances, so completed/
    // terminal history — which lived only in the read model — is restored here
    // from its durable home. Best-effort: an archive read failure must not abort
    // boot recovery of the (already reprojected) live state.
    match shard.replay_terminal_archive() {
        Ok(restored) if restored > 0 => tracing::info!(
            restored,
            "restored {restored} terminal/completed process instances from the durable \
             terminal-audit archive on top of the engine-snapshot reprojection (issue #831)"
        ),
        Ok(_) => {}
        Err(e) => tracing::warn!(
            error = %e,
            "could not replay the durable terminal-audit archive during reprojection \
             (issue #831); live instances are still recovered from the engine snapshot"
        ),
    }
    if total > 0 {
        shard
            .advance_exported(total as usize)
            .expect("advance exported_position to the recovered event count");
    }
}

/// Resets a shard and rebuilds it from just the surviving tail. Because
/// `exported_position` is an ABSOLUTE event index (compared against the
/// compaction `floor` on every boot), the cursor is advanced to `floor` before
/// the tail is projected — so it lands at `floor + surviving.len()`
/// (== `total_events`), not a relative `surviving.len()`. Skipping this would
/// leave the cursor below `floor` and re-trip [`CatchUpPlan::CompactedGap`] on
/// the very next boot, so even the opt-in lossy rebuild would never stick.
fn rebuild_from_surviving_tail(
    shard: &crate::readstore::ReadStore,
    floor: u64,
    surviving: &[&Event],
    export_ctx: &str,
) {
    shard.reset().expect("reset read store");
    if floor > 0 {
        shard
            .advance_exported(floor as usize)
            .expect("advance exported_position to the compaction floor");
    }
    if !surviving.is_empty() {
        shard.export(surviving).expect(export_ctx);
    }
}

#[cfg(test)]
mod tests {
    #[cfg(unix)]
    use std::os::unix::fs::PermissionsExt;

    use nanobpmn_engine_core::{Command, ProcessBuilder};

    use super::*;

    fn demo() -> nanobpmn_engine_core::ProcessDefinition {
        ProcessBuilder::new("demo")
            .start_event("start")
            .service_task("work", "demo-work")
            .end_event("end")
            .connect("start", "work")
            .connect("work", "end")
            .build()
            .expect("valid demo process")
    }

    fn temp_dir(tag: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-seglog-{tag}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = fs::remove_dir_all(&dir);
        dir
    }

    /// A fresh segmented dir adopts and recovers a deploy + instance across a
    /// reopen (back-compat: the active segment keeps the legacy `journal.jsonl`
    /// name).
    #[test]
    fn reopening_a_segmented_journal_replays_persisted_state() {
        let dir = temp_dir("roundtrip");

        let key = {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            assert!(recovery.fresh);
            assert!(journal.is_segmented());
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            // The active segment keeps the historical name.
            assert!(dir.join(ACTIVE_NAME).exists());
            events.iter().find_map(|e| e.instance_key()).unwrap()
        };

        let (reopened, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");
        assert!(!recovery.fresh);
        assert!(reopened.instance(key).is_some());
        assert_eq!(reopened.state().processes.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    /// Forcing a snapshot+rotate seals the active segment, persists a snapshot
    /// covering exactly that boundary, and compaction (at the snapshot
    /// watermark) deletes the sealed segment — while a reopen still recovers all
    /// state from snapshot + the surviving tail.
    #[test]
    fn snapshot_rotate_then_compaction_bounds_disk_and_recovers() {
        let dir = temp_dir("compaction");

        let (key1, key2, shared) = {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);

            // First instance: goes into the active segment.
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key1 = events.iter().find_map(|e| e.instance_key()).unwrap();

            // Snapshot + rotate: seals the active segment at the exact covered
            // boundary, so a sealed segment file now exists.
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
            assert!(covered > 0);
            assert_eq!(
                shared.sealed.lock().unwrap().len(),
                1,
                "rotate seals the active segment"
            );
            write_snapshot(&dir, snap, covered).expect("write snapshot");

            // Compaction at the snapshot watermark drops the sealed segment whose
            // events the snapshot now subsumes.
            let removed = compact(&shared, covered);
            assert_eq!(removed, 1, "the sealed prefix is compacted away");
            assert!(shared.sealed.lock().unwrap().is_empty());
            assert!(list_sealed(&dir).unwrap().is_empty());

            // Second instance: lands in the fresh active segment, after the
            // compacted prefix.
            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key2 = events.iter().find_map(|e| e.instance_key()).unwrap();

            (key1, key2, shared)
        };
        drop(shared);

        // Reopen: recovers from snapshot (covers key1's events, which were
        // compacted off disk) + active tail (key2).
        let (reopened, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");
        assert!(!recovery.fresh);
        assert!(
            reopened.instance(key1).is_some(),
            "snapshot restores the compacted instance"
        );
        assert!(
            reopened.instance(key2).is_some(),
            "the active tail restores the post-compaction instance"
        );
        assert_eq!(reopened.state().processes.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    /// Builds a segmented dir whose journal prefix has been compacted away:
    /// deploy + instance -> snapshot + rotate + compaction (drops the sealed
    /// prefix) -> a second instance in the fresh active segment. Returns the dir
    /// (all writer handles dropped, ready to reopen) and both instance keys.
    /// After this, the only complete source for the compacted prefix is the
    /// snapshot — exactly the state that turns a swallowed snapshot-decode error
    /// into a key-generator rewind (issue #1065).
    fn compacted_dir_with_two_instances(tag: &str) -> (PathBuf, u64, u64) {
        let dir = temp_dir(tag);
        let (key1, key2) = {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key1 = events.iter().find_map(|e| e.instance_key()).unwrap();

            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(
                compact(&shared, covered),
                1,
                "the sealed prefix is compacted"
            );
            assert!(list_sealed(&dir).unwrap().is_empty());

            let (events, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key2 = events.iter().find_map(|e| e.instance_key()).unwrap();
            (key1, key2)
        };
        (dir, key1, key2)
    }

    /// Returns the path of the (single) persisted snapshot file in `dir`.
    fn snapshot_file(dir: &Path) -> PathBuf {
        fs::read_dir(dir)
            .unwrap()
            .flatten()
            .map(|e| e.path())
            .find(|p| {
                p.file_name()
                    .and_then(|n| n.to_str())
                    .and_then(is_snap_file)
                    .is_some()
            })
            .expect("a snapshot file exists")
    }

    // NOTE (merge of epic/snapshot-durability → main): the former
    // `recover_rejects_a_present_but_undeserializable_snapshot` test was removed
    // here. It asserted the pre-epic contract (an undeserializable snapshot must
    // ABORT recovery). This epic supersedes that contract: a present-but-
    // undeserializable snapshot (schema drift) is now MIGRATED by replaying the
    // full history (cold archive + surviving tail) and rewriting a fresh in-format
    // snapshot, failing closed only on a genuine gap. That behaviour — including
    // the #1065 no-rewind guarantee — is owned by
    // `incompatible_snapshot_migrates_by_replay_preserving_key_high_water` and
    // `migration_fails_closed_on_pruned_gap`.

    /// RED/GREEN for the #1065 defect (raised in the #1066 review): a snapshot that is
    /// present but *unreadable by the OS* (e.g. wrong permissions) must fail loud **with
    /// the underlying OS error kind preserved** — operators need the actionable
    /// cause (`PermissionDenied`), not a re-mapped `InvalidData` that looks
    /// like schema drift.
    #[cfg(unix)]
    #[test]
    fn recover_reports_the_os_cause_for_an_unreadable_snapshot() {
        let (dir, _key1, _key2) = compacted_dir_with_two_instances("unreadable-snap");
        let snap = snapshot_file(&dir);
        fs::set_permissions(&snap, fs::Permissions::from_mode(0o000)).unwrap();

        let err = match crate::journal::Journal::open_segmented(&dir) {
            Ok(_) => {
                panic!("recovery must refuse an unreadable snapshot, not rewind the key space")
            }
            Err(e) => e,
        };
        assert_eq!(
            err.kind(),
            io::ErrorKind::PermissionDenied,
            "the OS error kind must survive the fail-loud wrapping: {err}"
        );

        fs::set_permissions(&snap, fs::Permissions::from_mode(0o600)).unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    /// Same guard for the multi-partition snapshot path: a present-but-unreadable
    /// multi-snapshot must fail loud with the OS error kind preserved.
    #[cfg(unix)]
    #[test]
    fn load_multi_snapshot_reports_the_os_cause_for_an_unreadable_file() {
        let dir = temp_dir("unreadable-msnap");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join(MULTI_SNAP_NAME);
        fs::write(&path, b"opaque").unwrap();
        fs::set_permissions(&path, fs::Permissions::from_mode(0o000)).unwrap();

        let err = load_multi_snapshot(&dir)
            .expect_err("an unreadable multi-snapshot must fail loud, not vanish");
        assert_eq!(
            err.kind(),
            io::ErrorKind::PermissionDenied,
            "the OS error kind must survive the fail-loud wrapping: {err}"
        );

        fs::set_permissions(&path, fs::Permissions::from_mode(0o600)).unwrap();
        let _ = fs::remove_dir_all(&dir);
    }

    // NOTE (merge of epic/snapshot-durability → main): the former
    // `recover_rejects_a_compacted_journal_with_no_snapshot` test was removed
    // here. It asserted the pre-epic contract (a compacted journal with no
    // snapshot must ABORT). This epic supersedes it: with no snapshot over a
    // compacted journal, recovery RECONSTRUCTS from the cold archive (+ surviving
    // tail), failing closed only when the cold archive cannot cover the gap. That
    // split contract is owned by `no_snapshot_over_compacted_journal_reconstructs_from_cold`
    // (reconstructs) and `no_snapshot_over_pruned_journal_fails_closed` (fails closed).

    /// RED/GREEN for the round-2 senior finding: a compacted journal with a
    /// *loadable but stale* snapshot — one whose `covered_events` sits **below**
    /// the compaction floor (`covered < first_index`) — must fail loud too. This
    /// is the same #1065 rewind reached through an older-than-floor snapshot
    /// instead of a missing/corrupt one: the newest snapshot is lost after
    /// compaction, an older one survives, `covered.saturating_sub(first_index)`
    /// clamps `skip` to 0, and the compacted `[covered, first_index)` window is
    /// silently dropped. The old `snapshot.is_none()` guard waved it straight
    /// through.
    #[test]
    fn recover_rejects_a_stale_snapshot_below_the_compaction_floor() {
        let (dir, _key1, _key2) = compacted_dir_with_two_instances("stale-snap");

        // Rewrite the surviving snapshot so it still deserializes but reports a
        // `covered_events` below the compaction floor (0 < first_index). Preserve
        // the envelope header line so `parse_envelope` + `check_format_version`
        // still admit the payload — the stale-floor guard, not a decode failure,
        // is what must reject it.
        let path = snapshot_file(&dir);
        let raw = fs::read(&path).unwrap();
        let nl = raw
            .iter()
            .position(|&b| b == b'\n')
            .expect("an enveloped snapshot carries a header line");
        let mut snap: PersistedSnapshot = serde_json::from_slice(&raw[nl + 1..]).unwrap();
        assert!(
            snap.covered_events > 0,
            "the compaction floor must be nonzero for this test to exercise the gap"
        );
        snap.covered_events = 0;
        let mut out = raw[..=nl].to_vec(); // header line + '\n'
        out.extend_from_slice(&serde_json::to_vec(&snap).unwrap());
        fs::write(&path, out).unwrap();

        let err = match crate::journal::Journal::open_segmented(&dir) {
            Ok(_) => panic!(
                "recovery must refuse a stale snapshot below the compaction floor, not rewind the key space"
            ),
            Err(e) => e,
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_dir_all(&dir);
    }

    /// GREEN guard against the key rewind itself: a *valid* snapshot recovery
    /// preserves the key high-water, so the next minted instance key is strictly
    /// greater than the last one minted before the restart — never a rewound low
    /// key like the incident's `41`.
    #[test]
    fn recover_preserves_the_key_high_water_across_a_valid_snapshot() {
        let (dir, key1, key2) = compacted_dir_with_two_instances("high-water");
        assert!(key2 > key1);

        let next_key = {
            let (mut reopened, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");
            assert!(!recovery.fresh);
            assert!(reopened.instance(key1).is_some(), "snapshot restores key1");
            assert!(
                reopened.instance(key2).is_some(),
                "active tail restores key2"
            );
            let (events, _) = reopened
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            events.iter().find_map(|e| e.instance_key()).unwrap()
        };
        assert!(
            next_key > key2,
            "the key generator must not rewind across recovery: minted {next_key} <= prior {key2}"
        );

        let _ = fs::remove_dir_all(&dir);
    }
    #[test]
    fn compaction_respects_the_watermark() {
        let dir = temp_dir("watermark");

        let (mut journal, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("open segmented");
        let shared = Arc::clone(&recovery.shared);

        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let _ = journal
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        let (_snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
        assert_eq!(shared.sealed.lock().unwrap().len(), 1);

        // A watermark of 0 (read model has exported nothing) removes nothing.
        assert_eq!(compact(&shared, 0), 0);
        assert_eq!(shared.sealed.lock().unwrap().len(), 1);

        // Just below the segment boundary: still retained.
        assert_eq!(compact(&shared, covered - 1), 0);
        assert_eq!(shared.sealed.lock().unwrap().len(), 1);

        // At the boundary: removed.
        assert_eq!(compact(&shared, covered), 1);
        assert!(shared.sealed.lock().unwrap().is_empty());

        drop(journal);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Recovery from snapshot + tail reconstructs exactly the same engine state
    /// as a full replay of every event (segment roundtrip + snapshot fidelity).
    #[test]
    fn boot_from_snapshot_matches_full_replay() {
        let dir = temp_dir("fidelity");

        let (keys, all_events) = {
            let (mut journal, _recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let (deploy_events, _) = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let mut keys = Vec::new();
            let mut all_events: Vec<Event> = deploy_events.iter().cloned().collect();
            for _ in 0..5 {
                let (events, _) = journal
                    .apply_command(Command::create_instance("demo"))
                    .unwrap();
                keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
                all_events.extend(events.iter().cloned());
            }
            // Snapshot + rotate midway, so recovery must fuse snapshot + tail.
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            // A couple more instances after the snapshot boundary.
            for _ in 0..2 {
                let (events, _) = journal
                    .apply_command(Command::create_instance("demo"))
                    .unwrap();
                keys.push(events.iter().find_map(|e| e.instance_key()).unwrap());
                all_events.extend(events.iter().cloned());
            }
            (keys, all_events)
        };

        // Full-replay reference engine.
        let reference = Engine::replay_partition(0, all_events);

        // Snapshot+tail recovery via reopen.
        let (journal, _recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");

        for key in &keys {
            assert_eq!(
                journal.instance(*key).is_some(),
                reference.instance(*key).is_some(),
                "instance {key:?} presence must match full replay"
            );
        }
        assert_eq!(
            journal.state().processes.len(),
            reference.state().processes.len()
        );

        drop(journal);
        let _ = fs::remove_dir_all(&dir);
    }

    /// End-to-end multi-partition bounded-disk cycle: two partitions share one
    /// segmented WAL; a snapshot of every partition + a combined snapshot lets
    /// per-partition compaction drop the sealed prefix, and a reopen restores
    /// every partition's instances from the combined snapshot.
    #[test]
    fn multi_partition_snapshot_compaction_and_recovery() {
        let dir = temp_dir("multi-roundtrip");
        // Keep the exporter receiver alive so shared writes have a wired cell.
        let (tx, _rx) = std::sync::mpsc::channel::<crate::journal::ExportBatch>();

        let (key0, key1) = {
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[0, 1], 2, None)
                    .expect("open multi");
            let seg = Arc::clone(&recovery.shared);
            assert!(recovery.fresh);
            let mut engines: std::collections::HashMap<u64, Engine> =
                recovery.engines.into_iter().collect();
            let mut j0 = crate::journal::Journal::from_engine_shared(
                0,
                engines.remove(&0).unwrap(),
                true,
                &writer,
            );
            let mut j1 = crate::journal::Journal::from_engine_shared(
                1,
                engines.remove(&1).unwrap(),
                true,
                &writer,
            );
            j0.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));

            // Deploy on partition 0, replicate the definition in-memory to p1.
            let (deploy_events, _) = j0.apply_command(Command::DeployProcess(demo())).unwrap();
            j1.install_deployment(&deploy_events);

            let (e0, _) = j0.apply_command(Command::create_instance("demo")).unwrap();
            let key0 = e0.iter().find_map(|e| e.instance_key()).unwrap();
            let (e1, _) = j1.apply_command(Command::create_instance("demo")).unwrap();
            let key1 = e1.iter().find_map(|e| e.instance_key()).unwrap();
            assert_eq!(nanobpmn_engine_core::partition_of(key1), 1);

            // Snapshot each partition (each seals the shared active segment; only
            // the first seal produces a non-empty sealed segment).
            let (snap0, covered0) = j0.snapshot_and_rotate().expect("snapshot p0");
            let (snap1, covered1) = j1.snapshot_and_rotate().expect("snapshot p1");
            assert_eq!(seg.sealed.lock().unwrap().len(), 1);

            write_multi_snapshot(&dir, vec![(0, covered0, snap0), (1, covered1, snap1)])
                .expect("write combined snapshot");

            // Below partition 1's watermark: retained (the snapshot for p1 does
            // not yet subsume its events in the sealed segment).
            let held_back = [covered0, covered1.saturating_sub(1)];
            assert_eq!(compact_multi(&seg, &held_back, &[u64::MAX; 2]), 0);
            assert_eq!(seg.sealed.lock().unwrap().len(), 1);

            // With every partition's watermark met AND the read model past the
            // segment: compacted away.
            let covered = [covered0, covered1];
            // Snapshot subsumes the segment for every partition, but p1's
            // read-model shard has not yet projected its events in the segment:
            // the per-partition export gate keeps it.
            let exported_lag = [u64::MAX, covered1.saturating_sub(1)];
            assert_eq!(compact_multi(&seg, &covered, &exported_lag), 0);
            assert_eq!(seg.sealed.lock().unwrap().len(), 1);
            assert_eq!(compact_multi(&seg, &covered, &[u64::MAX; 2]), 1);
            assert!(seg.sealed.lock().unwrap().is_empty());

            (key0, key1)
        };

        // Reopen: both partitions restore purely from the combined snapshot (the
        // sealed prefix was compacted off disk).
        let recovery = recover_multi(&dir, &[0, 1], 2, None).expect("reopen multi");
        assert!(!recovery.fresh);
        let engines: std::collections::HashMap<u64, Engine> =
            recovery.engines.into_iter().collect();
        assert!(
            engines[&0].instance(key0).is_some(),
            "partition 0 instance restored from snapshot"
        );
        assert!(
            engines[&1].instance(key1).is_some(),
            "partition 1 instance restored from snapshot"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// RED/GREEN for the multi-partition arm of the round-2 senior finding: a
    /// compacted shared journal whose combined snapshot loads but is *stale for
    /// one owned partition* (`covered < pp_base[p]`) must fail loud. Reached
    /// exactly as #1065 per shard: `covered.saturating_sub(base_p)` clamps that
    /// partition's `skip` to 0, replaying only its surviving tail across the
    /// compacted gap and rewinding its key generator. The old `combined.is_none()`
    /// guard only caught a wholly-absent multi-snapshot, not a per-partition gap.
    #[test]
    fn recover_multi_rejects_a_stale_partition_snapshot_below_the_compaction_floor() {
        let dir = temp_dir("multi-stale-snap");
        let (tx, _rx) = std::sync::mpsc::channel::<crate::journal::ExportBatch>();

        {
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[0, 1], 2, None)
                    .expect("open multi");
            let seg = Arc::clone(&recovery.shared);
            let mut engines: std::collections::HashMap<u64, Engine> =
                recovery.engines.into_iter().collect();
            let mut j0 = crate::journal::Journal::from_engine_shared(
                0,
                engines.remove(&0).unwrap(),
                true,
                &writer,
            );
            let mut j1 = crate::journal::Journal::from_engine_shared(
                1,
                engines.remove(&1).unwrap(),
                true,
                &writer,
            );
            j0.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));

            let (deploy_events, _) = j0.apply_command(Command::DeployProcess(demo())).unwrap();
            j1.install_deployment(&deploy_events);
            let _ = j0.apply_command(Command::create_instance("demo")).unwrap();
            let _ = j1.apply_command(Command::create_instance("demo")).unwrap();

            let (snap0, covered0) = j0.snapshot_and_rotate().expect("snapshot p0");
            let (snap1, covered1) = j1.snapshot_and_rotate().expect("snapshot p1");
            write_multi_snapshot(&dir, vec![(0, covered0, snap0), (1, covered1, snap1)])
                .expect("write combined snapshot");
            assert_eq!(
                compact_multi(&seg, &[covered0, covered1], &[u64::MAX; 2]),
                1,
                "the sealed prefix is compacted for both partitions"
            );
            assert!(seg.sealed.lock().unwrap().is_empty());
        }

        // Simulate the loadable-but-stale case: rewrite partition 1's entry so its
        // `covered` sits below the compaction floor (0 < pp_base[1]). Preserve the
        // envelope header line so the payload still parses — the per-partition
        // stale-floor guard is what must reject it.
        let path = dir.join(MULTI_SNAP_NAME);
        let raw = fs::read(&path).unwrap();
        let nl = raw
            .iter()
            .position(|&b| b == b'\n')
            .expect("an enveloped multi-snapshot carries a header line");
        let mut snap: MultiPersistedSnapshot = serde_json::from_slice(&raw[nl + 1..]).unwrap();
        let e = snap
            .entries
            .iter_mut()
            .find(|e| e.partition == 1)
            .expect("partition 1 snapshot entry");
        assert!(
            e.covered > 0,
            "partition 1's compaction floor must be nonzero for this test to exercise the gap"
        );
        e.covered = 0;
        let mut out = raw[..=nl].to_vec(); // header line + '\n'
        out.extend_from_slice(&serde_json::to_vec(&snap).unwrap());
        fs::write(&path, out).unwrap();

        let err = recover_multi(&dir, &[0, 1], 2, None)
            .err()
            .expect("recovery must refuse a stale partition snapshot, not rewind the key space");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn multi_partition_recovery_fuses_snapshot_and_tail() {
        let dir = temp_dir("multi-tail");
        let (tx, _rx) = std::sync::mpsc::channel::<crate::journal::ExportBatch>();

        let (pre0, post1) = {
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[0, 1], 2, None)
                    .expect("open multi");
            let mut engines: std::collections::HashMap<u64, Engine> =
                recovery.engines.into_iter().collect();
            let mut j0 = crate::journal::Journal::from_engine_shared(
                0,
                engines.remove(&0).unwrap(),
                true,
                &writer,
            );
            let mut j1 = crate::journal::Journal::from_engine_shared(
                1,
                engines.remove(&1).unwrap(),
                true,
                &writer,
            );
            j0.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));

            let (deploy_events, _) = j0.apply_command(Command::DeployProcess(demo())).unwrap();
            j1.install_deployment(&deploy_events);

            // Pre-snapshot instance on partition 0.
            let (e0, _) = j0.apply_command(Command::create_instance("demo")).unwrap();
            let pre0 = e0.iter().find_map(|e| e.instance_key()).unwrap();

            let (snap0, covered0) = j0.snapshot_and_rotate().expect("snapshot p0");
            let (snap1, covered1) = j1.snapshot_and_rotate().expect("snapshot p1");
            write_multi_snapshot(&dir, vec![(0, covered0, snap0), (1, covered1, snap1)])
                .expect("write combined snapshot");

            // Post-snapshot instance on partition 1 (lands in the fresh active
            // tail, not covered by any snapshot). Await its commit so the write
            // is durable before we drop the writer and recover (otherwise it
            // races the background group-commit thread).
            let (e1, commit) = j1.apply_command(Command::create_instance("demo")).unwrap();
            let post1 = e1.iter().find_map(|e| e.instance_key()).unwrap();
            commit.blocking_wait();

            (pre0, post1)
        };

        let recovery = recover_multi(&dir, &[0, 1], 2, None).expect("reopen multi");
        assert!(!recovery.fresh);
        let engines: std::collections::HashMap<u64, Engine> =
            recovery.engines.into_iter().collect();
        assert!(
            engines[&0].instance(pre0).is_some(),
            "pre-snapshot instance from snapshot"
        );
        assert!(
            engines[&1].instance(post1).is_some(),
            "post-snapshot instance from the surviving tail"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Lean-snapshot recovery: with an authoritative var store wired, the
    /// periodic checkpoint captures a **control-only** snapshot (no variables) and
    /// writes the variable delta to the store. Recovery must fuse the control
    /// snapshot + the store's variables + the journal tail so the restored
    /// variables exactly match a full replay — including a `SetVariables` applied
    /// AFTER the checkpoint (which merges onto the store's base on replay).
    #[test]
    fn lean_snapshot_recovery_matches_full_replay() {
        use std::collections::HashMap;

        use nanobpmn_engine_core::Value;

        let dir = temp_dir("lean-roundtrip");
        let (tx, _rx) = std::sync::mpsc::channel::<crate::journal::ExportBatch>();
        // Authoritative, boot-surviving store. In-process the same Arc models the
        // durable store surviving the journal reopen.
        let varstore = Arc::new(crate::varstore::VarStore::open(None).expect("open var store"));

        let key = {
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[0, 1], 2, Some(&varstore))
                    .expect("open multi");
            assert!(recovery.fresh);
            let mut engines: std::collections::HashMap<u64, Engine> =
                recovery.engines.into_iter().collect();
            let mut j0 = crate::journal::Journal::from_engine_shared(
                0,
                engines.remove(&0).unwrap(),
                true,
                &writer,
            );
            let mut j1 = crate::journal::Journal::from_engine_shared(
                1,
                engines.remove(&1).unwrap(),
                true,
                &writer,
            );
            j0.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            // Lean mode: the store is authoritative for variables on both journals.
            j0.set_varstore(Arc::clone(&varstore));
            j1.set_varstore(Arc::clone(&varstore));

            let (deploy_events, _) = j0.apply_command(Command::DeployProcess(demo())).unwrap();
            j1.install_deployment(&deploy_events);

            // Create with initial variables, then merge a second batch — all
            // BEFORE the checkpoint (so they live only in the store + control
            // snapshot, never in a variable-bearing snapshot).
            let mut init = HashMap::new();
            init.insert("a".to_string(), Value::Int(1i64));
            let (e0, _) = j0
                .apply_command(Command::create_instance_with("demo", init))
                .unwrap();
            let key = e0.iter().find_map(|e| e.instance_key()).unwrap();
            let mut pre = HashMap::new();
            pre.insert("b".to_string(), Value::Int(2i64));
            let _ = j0.apply_command(Command::set_variables(key, pre)).unwrap();

            // Lean checkpoint on both partitions: drain + control-only snapshot +
            // seal, then persist the delta to the store BEFORE writing the snapshot.
            let mut entries = Vec::new();
            for (j, pid) in [(&mut j0, 0u64), (&mut j1, 1u64)] {
                let (snap, covered, upserts, forgets) =
                    j.snapshot_and_rotate_lean().expect("lean checkpoint");
                let ups: Vec<(nanobpmn_engine_core::Key, &HashMap<String, Value>)> =
                    upserts.iter().map(|(k, v)| (*k, v.as_ref())).collect();
                varstore.checkpoint(pid, covered, &ups, &forgets).unwrap();
                entries.push((pid, covered, snap));
            }
            write_multi_snapshot(&dir, entries).expect("write lean snapshot");

            // Post-checkpoint variable merge: lands in the fresh active tail (NOT
            // covered by the snapshot, NOT yet in the store's checkpoint) — the
            // merge-onto-base case recovery must get right. Await its commit so
            // the write is durable in the segment before we drop the writer and
            // recover (otherwise it races the background group-commit thread).
            let mut post = HashMap::new();
            post.insert("c".to_string(), Value::Int(3i64));
            let (_, commit) = j0.apply_command(Command::set_variables(key, post)).unwrap();
            commit.blocking_wait();

            key
        };

        // Reopen: control from the lean snapshot, variables installed from the
        // store, then the tail `SetVariables` merged on top.
        let recovery = recover_multi(&dir, &[0, 1], 2, Some(&varstore)).expect("reopen multi");
        assert!(!recovery.fresh);
        let engines: std::collections::HashMap<u64, Engine> =
            recovery.engines.into_iter().collect();
        let inst = engines[&0].instance(key).expect("instance restored");
        assert_eq!(
            inst.variables.get("a"),
            Some(&Value::Int(1i64)),
            "create-time variable from the store"
        );
        assert_eq!(
            inst.variables.get("b"),
            Some(&Value::Int(2i64)),
            "pre-checkpoint merge from the store"
        );
        assert_eq!(
            inst.variables.get("c"),
            Some(&Value::Int(3i64)),
            "post-checkpoint merge from the journal tail"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Clustered recovery: a node owning partitions {1,2} (NOT the deployment
    /// partition 0) journals a single durable replicated `ProcessDeployed` under
    /// its first-owned partition (1), keyed to partition 0. On recovery, the
    /// write TAG routes it back to partition 1, and the version-guarded broadcast
    /// re-installs the partition-agnostic definition into partition 2, so both
    /// partitions can restore instances that reference it.
    #[test]
    fn clustered_replicated_deployment_recovers_across_owned_partitions() {
        let dir = temp_dir("cluster-deploy");
        let (tx, _rx) = std::sync::mpsc::channel::<crate::journal::ExportBatch>();

        // Mint a deployment on partition 0 (the definition's home) to obtain the
        // partition-0-keyed `ProcessDeployed` events a peer node would receive.
        let deploy_events: Vec<Event> = {
            let mut p0 = crate::journal::Journal::in_memory_partition(0);
            let (evs, _) = p0.apply_command(Command::DeployProcess(demo())).unwrap();
            evs.iter().cloned().collect()
        };
        assert!(deploy_events.iter().all(|e| partition_of(e.max_key()) == 0));

        let (inst1, inst2) = {
            // This node owns {1,2}; the shared segmented WAL spans a 3-partition
            // cluster.
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[1, 2], 3, None)
                    .expect("open clustered");
            let mut engines: std::collections::HashMap<u64, Engine> =
                recovery.engines.into_iter().collect();
            let mut j1 = crate::journal::Journal::from_engine_shared(
                1,
                engines.remove(&1).unwrap(),
                true,
                &writer,
            );
            let mut j2 = crate::journal::Journal::from_engine_shared(
                2,
                engines.remove(&2).unwrap(),
                true,
                &writer,
            );
            j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            j2.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));

            // Durable copy on the first-owned partition (tagged 1, keyed 0);
            // in-memory on the rest. Dropping the writer at block end flushes it.
            let _ = j1.install_deployment_durable(&deploy_events);
            j2.install_deployment(&deploy_events);

            let (e1, _) = j1.apply_command(Command::create_instance("demo")).unwrap();
            let inst1 = e1.iter().find_map(|e| e.instance_key()).unwrap();
            let (e2, _) = j2.apply_command(Command::create_instance("demo")).unwrap();
            let inst2 = e2.iter().find_map(|e| e.instance_key()).unwrap();
            assert_eq!(partition_of(inst1), 1);
            assert_eq!(partition_of(inst2), 2);

            // Flush the detached writer deterministically: a rotate is a writer
            // barrier (blocks until every prior write is fsynced). We seal but do
            // NOT write a combined snapshot, so recovery rebuilds by full replay —
            // exercising the broadcast as partition 2's SOLE definition source.
            let _ = j1.snapshot_and_rotate().expect("flush via rotate");

            (inst1, inst2)
        };

        // Reopen as the same clustered node: both partitions must restore the
        // definition (p1 from its tagged durable copy, p2 from the broadcast) and
        // their instances.
        let recovery = recover_multi(&dir, &[1, 2], 3, None).expect("reopen clustered");
        assert!(!recovery.fresh);
        let engines: std::collections::HashMap<u64, Engine> =
            recovery.engines.into_iter().collect();
        assert!(
            engines[&1].instance(inst1).is_some(),
            "partition 1 instance restored"
        );
        assert!(
            engines[&2].instance(inst2).is_some(),
            "partition 2 instance restored (definition arrived via broadcast)"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// A large, compressible group-commit batch survives the value-codec
    /// round-trip: `frame_compress` shrinks it (the point) and
    /// `decode_segment_bytes` reconstructs the exact original bytes.
    #[test]
    fn journal_frame_compress_roundtrips_and_shrinks() {
        let dir = temp_dir("frame-roundtrip");
        fs::create_dir_all(&dir).unwrap();

        // Realistic-ish repeated JSON lines: highly compressible, one big event.
        let mut buf = Vec::new();
        for i in 0..64 {
            buf.extend_from_slice(
                format!(
                    "0\t{{\"seq\":{i},\"payload\":\"{}\"}}\n",
                    "abcdefgh".repeat(256)
                )
                .as_bytes(),
            );
        }
        assert!(buf.len() >= MIN_FRAME_BYTES, "batch big enough to frame");

        let frame = frame_compress(&buf, 64).expect("large compressible batch frames");
        assert!(
            frame.len() < buf.len(),
            "frame shrank the physical write: {} -> {}",
            buf.len(),
            frame.len()
        );
        assert_eq!(frame[0], FRAME_MAGIC);
        assert_eq!(frame[1], CODEC_DEFLATE);

        let path = dir.join("frame.bin");
        fs::write(&path, &frame).unwrap();
        assert_eq!(
            decode_segment_bytes(&path).unwrap(),
            buf,
            "exact round-trip"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// The size/mean-event gates keep the negligible/high-rate regime verbatim:
    /// a small batch (or one of tiny events) declines framing, so the writer
    /// thread is never taxed and the segment stays legacy plaintext.
    #[test]
    fn journal_frame_compress_declines_small_batches() {
        // Below the byte floor.
        assert!(frame_compress(b"0\t{}\n", 1).is_none());
        // Past the byte floor but the mean event is tiny (many small events).
        let many_small: Vec<u8> = std::iter::repeat_n(b"0\t{\"x\":1}\n", 4096)
            .flatten()
            .copied()
            .collect();
        assert!(many_small.len() >= MIN_FRAME_BYTES);
        assert!(
            frame_compress(&many_small, 4096).is_none(),
            "tiny mean event skips compression"
        );
    }

    /// A segment may interleave legacy plaintext lines and compressed frames
    /// (the flag can flip across a restart while the same active segment is
    /// open). `decode_segment_bytes` walks record-by-record and reconstructs the
    /// concatenated logical stream regardless of the mix.
    #[test]
    fn journal_decode_handles_interleaved_plaintext_and_frames() {
        let dir = temp_dir("frame-interleave");
        fs::create_dir_all(&dir).unwrap();

        let head = b"0\t{\"seq\":\"head\"}\n".to_vec();
        let mut mid = Vec::new();
        for i in 0..40 {
            mid.extend_from_slice(
                format!("0\t{{\"seq\":{i},\"p\":\"{}\"}}\n", "z".repeat(1400)).as_bytes(),
            );
        }
        let tail = b"0\t{\"seq\":\"tail\"}\n".to_vec();

        let frame = frame_compress(&mid, 40).expect("mid batch frames");
        let mut file = Vec::new();
        file.extend_from_slice(&head); // legacy plaintext
        file.extend_from_slice(&frame); // compressed frame
        file.extend_from_slice(&tail); // legacy plaintext again

        let path = dir.join("mixed.jsonl");
        fs::write(&path, &file).unwrap();

        let mut expected = head.clone();
        expected.extend_from_slice(&mid);
        expected.extend_from_slice(&tail);
        assert_eq!(decode_segment_bytes(&path).unwrap(), expected);

        let _ = fs::remove_dir_all(&dir);
    }

    /// A torn trailing frame (crash mid-append, never fsynced/acked) is dropped:
    /// decode returns the durable prefix and never errors, matching the writer's
    /// ack-after-write-before-fsync contract.
    #[test]
    fn journal_decode_drops_a_torn_frame_tail() {
        let dir = temp_dir("frame-torn");
        fs::create_dir_all(&dir).unwrap();

        let durable = b"0\t{\"seq\":\"durable\"}\n".to_vec();
        let mut big = Vec::new();
        for i in 0..40 {
            big.extend_from_slice(
                format!("0\t{{\"seq\":{i},\"p\":\"{}\"}}\n", "q".repeat(1400)).as_bytes(),
            );
        }
        let frame = frame_compress(&big, 40).expect("frames");

        // Truncate the frame mid-body to simulate a torn write.
        let mut file = durable.clone();
        file.extend_from_slice(&frame[..frame.len() - 4]);

        let path = dir.join("torn.jsonl");
        fs::write(&path, &file).unwrap();
        assert_eq!(
            decode_segment_bytes(&path).unwrap(),
            durable,
            "durable prefix survives, torn frame dropped"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// End-to-end through the segment reader: a compressed active segment written
    /// by `ActiveSegment::write_all` (with `compress` on) recovers the exact same
    /// events as the plaintext path.
    #[test]
    fn compressed_active_segment_recovers_events() {
        let dir = temp_dir("frame-active");
        fs::create_dir_all(&dir).unwrap();

        // Serialize real events by minting them through an in-memory journal, so
        // the reader exercises the true event JSON shape.
        let mut j = crate::journal::Journal::in_memory_partition(0);
        let (deploy, _) = j.apply_command(Command::DeployProcess(demo())).unwrap();
        let mut lines = Vec::new();
        let mut n_events: u64 = 0;
        for e in deploy.iter() {
            lines.extend_from_slice(serde_json::to_string(e).unwrap().as_bytes());
            lines.push(b'\n');
            n_events += 1;
        }
        for _ in 0..80 {
            let mut vars = std::collections::HashMap::new();
            vars.insert(
                "payload".to_string(),
                nanobpmn_engine_core::Value::Str("y".repeat(16384)),
            );
            let (evs, _) = j
                .apply_command(Command::create_instance_with("demo", vars))
                .unwrap();
            for e in evs.iter() {
                lines.extend_from_slice(serde_json::to_string(e).unwrap().as_bytes());
                lines.push(b'\n');
                n_events += 1;
            }
        }

        // Baseline: plaintext segment.
        let plain_path = dir.join("plain.jsonl");
        fs::write(&plain_path, &lines).unwrap();
        let baseline = read_segment_events(&plain_path).unwrap();
        assert_eq!(baseline.len() as u64, n_events);

        // Compressed active segment via the real writer path.
        let active_path = dir.join(ACTIVE_NAME);
        let shared = Arc::new(SegShared {
            dir: dir.clone(),
            active_path: active_path.clone(),
            total_events: AtomicU64::new(0),
            active_start: AtomicU64::new(0),
            sealed: Mutex::new(Vec::new()),
            segment_bytes: u64::MAX,
            per_partition_total: Vec::new(),
            per_partition_active_start: Vec::new(),
            compress: true,
        });
        {
            let mut seg = ActiveSegment::open(Arc::clone(&shared)).unwrap();
            seg.write_all(&lines, n_events).unwrap();
            seg.fsync().unwrap();
        }
        // The on-disk segment must actually be a compressed frame, not plaintext.
        let on_disk = fs::read(&active_path).unwrap();
        assert_eq!(
            on_disk[0], FRAME_MAGIC,
            "active segment is frame-compressed"
        );
        assert!(on_disk.len() < lines.len(), "physical write shrank");

        let recovered = read_segment_events(&active_path).unwrap();
        assert_eq!(recovered.len(), baseline.len());
        // The strongest guarantee: the compressed segment decodes to the exact
        // bytes a plaintext write would have produced (re-serializing parsed
        // events would be flaky — the process definition's element map has
        // nondeterministic iteration order).
        assert_eq!(
            decode_segment_bytes(&active_path).unwrap(),
            lines,
            "compressed active segment decodes byte-for-byte to the plaintext batch"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// A crash mid-append of a plaintext batch leaves the final record without a
    /// terminating newline. `decode_segment_bytes` drops that torn tail (never
    /// hands a truncated line to the reader) and keeps the durable prefix.
    #[test]
    fn journal_decode_drops_a_torn_plaintext_tail() {
        let dir = temp_dir("plain-torn");
        fs::create_dir_all(&dir).unwrap();

        let durable = b"0\t{\"seq\":\"one\"}\n0\t{\"seq\":\"two\"}\n".to_vec();
        let mut file = durable.clone();
        // Unterminated (torn) trailing line — no `\n`.
        file.extend_from_slice(b"0\t{\"seq\":\"tor");

        let path = dir.join("torn-plain.jsonl");
        fs::write(&path, &file).unwrap();
        assert_eq!(
            decode_segment_bytes(&path).unwrap(),
            durable,
            "durable prefix survives, torn unterminated line dropped"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Boot recovery must never panic on a torn journal tail. Both a truncated
    /// final record and NUL padding at end-of-file (a preallocated file killed
    /// mid-write) recover the durable prefix instead of erroring — but genuine
    /// corruption before the final line still errors.
    #[test]
    fn read_segment_tolerates_torn_and_nul_padded_tail() {
        let dir = temp_dir("seg-torn-tail");
        fs::create_dir_all(&dir).unwrap();

        // Mint two real events so the reader exercises the true JSON shape.
        let mut j = crate::journal::Journal::in_memory_partition(0);
        let (deploy, _) = j.apply_command(Command::DeployProcess(demo())).unwrap();
        let (create, _) = j
            .apply_command(Command::create_instance_with(
                "demo",
                std::collections::HashMap::new(),
            ))
            .unwrap();
        let mut good = Vec::new();
        let mut n: u64 = 0;
        for e in deploy.iter().chain(create.iter()) {
            good.extend_from_slice(serde_json::to_string(e).unwrap().as_bytes());
            good.push(b'\n');
            n += 1;
        }

        // Case 1: truncated final record (no newline).
        let mut torn = good.clone();
        torn.extend_from_slice(br#"{"CreateInstance":{"process":"de"#);
        let p1 = dir.join("torn.jsonl");
        fs::write(&p1, &torn).unwrap();
        assert_eq!(
            read_segment_events(&p1).unwrap().len() as u64,
            n,
            "truncated tail dropped, durable events recovered"
        );

        // Case 2: a fully newline-terminated but unparseable final line — the
        // shape a torn write leaves when reused-block garbage contains a `\n`, so
        // it survives `decode_segment_bytes` and must be dropped by the reader.
        let mut garbage = good.clone();
        garbage.extend_from_slice(b"}\x00torngarbage{not-json\n");
        let p2 = dir.join("garbage.jsonl");
        fs::write(&p2, &garbage).unwrap();
        assert_eq!(
            read_segment_events(&p2).unwrap().len() as u64,
            n,
            "terminated-but-unparseable tail dropped, durable events recovered"
        );

        // Case 3: corruption BEFORE the final valid line still errors — only the
        // tail is allowed to be torn.
        let mut mid_corrupt = Vec::new();
        mid_corrupt.extend_from_slice(b"{ this is not valid json");
        mid_corrupt.push(b'\n');
        mid_corrupt.extend_from_slice(&good);
        let p3 = dir.join("midcorrupt.jsonl");
        fs::write(&p3, &mid_corrupt).unwrap();
        assert!(
            read_segment_events(&p3).is_err(),
            "mid-file corruption is not silently dropped"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// The catch-up classifier is the single source of truth for the three boot
    /// sites. In the normal window it resumes; above the tail it rebuilds; and —
    /// the issue #600 guard — a store *below* the compaction floor is a
    /// `CompactedGap`, never a silent `Resume`.
    #[test]
    fn plan_catch_up_classifies_every_position() {
        // Fresh dir (no compaction): floor 0, resume from the start.
        assert_eq!(plan_catch_up(0, 0, 5), CatchUpPlan::Resume { skip: 0 });
        // Warm resume inside the surviving window.
        assert_eq!(
            plan_catch_up(4850, 4848, 10),
            CatchUpPlan::Resume { skip: 2 }
        );
        // Exactly at the floor: resume, replaying the whole surviving tail.
        assert_eq!(
            plan_catch_up(4848, 4848, 10),
            CatchUpPlan::Resume { skip: 0 }
        );
        // Exactly at the tail end: resume with nothing left to project.
        assert_eq!(
            plan_catch_up(4858, 4848, 10),
            CatchUpPlan::Resume { skip: 10 }
        );
        // Past the tail (truncated/corrupt log): rebuild from what survives.
        assert_eq!(
            plan_catch_up(4859, 4848, 10),
            CatchUpPlan::RebuildFromSurviving
        );
        // Below the floor (wiped/reset read model over a compacted journal): the
        // defect. Must be a gap, NOT `Resume { skip: 0 }` (which silently drops
        // the compacted-away history — issue #600).
        assert_eq!(
            plan_catch_up(0, 4848, 10),
            CatchUpPlan::CompactedGap { missing: 4848 }
        );
        assert_eq!(
            plan_catch_up(4000, 4848, 10),
            CatchUpPlan::CompactedGap { missing: 848 }
        );
    }

    /// End-to-end reproduction of issue #600: a read model wiped below the
    /// journal's compaction floor is **refused** (default) rather than silently
    /// resumed into a partial/empty projection; with the opt-in escape hatch it
    /// rebuilds loudly from the surviving tail.
    #[test]
    fn wiped_read_model_over_compacted_journal_is_refused() {
        use nanobpmn_engine_core::Command;

        use crate::readstore::ReadStore;

        let dir = temp_dir("readmodel-gap-600");

        // Build a segmented journal, snapshot+rotate+compact so history moves
        // into the snapshot and `first_index > 0`, then add a self-consistent
        // post-compaction tail (re-deploy + instance) that projects cleanly.
        {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(compact(&shared, covered), 1, "sealed prefix compacted away");
            // Post-compaction, self-consistent tail.
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
        }

        // Reopen: the surviving tail sits above a non-zero compaction floor.
        let (_engine, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");
        assert!(
            recovery.first_index > 0,
            "compaction must leave a non-zero floor to reproduce the gap"
        );
        let surviving: Vec<&Event> = recovery.events.iter().collect();

        // A freshly wiped read model reports `exported_position == 0`, i.e. below
        // the compaction floor — the exact state after a schema-fingerprint wipe.
        let store = ReadStore::open(None).expect("fresh in-memory read store");
        assert_eq!(store.exported_position(), 0);

        // Default: refuse. Suppress the panic hook so the expected abort doesn't
        // spam the test log.
        let prev = std::panic::take_hook();
        std::panic::set_hook(Box::new(|_| {}));
        let refused = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            catch_up_shard(&store, recovery.first_index, &surviving, None);
        }));
        std::panic::set_hook(prev);
        assert!(
            refused.is_err(),
            "a read model below the compaction floor must abort, not silently resume"
        );
        assert_eq!(
            store.exported_position(),
            0,
            "the refused catch-up must not have advanced the read model"
        );

        // Opt-in escape hatch: rebuild from the surviving tail (lossy, but loud).
        // Safe from cross-test env races: this is the only test touching this var.
        unsafe { std::env::set_var("NANOBPMN_READ_MODEL_LOSSY_REBUILD", "1") };
        let rebuilt = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            catch_up_shard(&store, recovery.first_index, &surviving, None);
        }));
        unsafe { std::env::remove_var("NANOBPMN_READ_MODEL_LOSSY_REBUILD") };
        assert!(rebuilt.is_ok(), "the escape hatch must rebuild, not abort");
        assert_eq!(
            store.exported_position() as u64,
            recovery.total_events,
            "lossy rebuild must leave the cursor at the ABSOLUTE tail end \
             (floor + surviving.len() == total_events), not a relative surviving.len()"
        );
        // Regression guard for the re-abort loop: with the cursor now at the
        // absolute tail, a subsequent boot resumes cleanly instead of re-tripping
        // CompactedGap — even without the escape hatch set.
        assert_eq!(
            plan_catch_up(
                store.exported_position() as u64,
                recovery.first_index,
                surviving.len() as u64,
            ),
            CatchUpPlan::Resume {
                skip: surviving.len()
            },
            "the rebuilt cursor must resume, not re-abort, on the next boot"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Issue #732: a read model wiped below the journal compaction floor is
    /// REPROJECTED from the authoritative engine snapshot (the default when the
    /// boot engine state is available) — no panic, no lossy env flag — recovering
    /// every live entity, and lands the absolute cursor at `total_events`.
    #[test]
    fn wiped_read_model_reprojects_from_engine_snapshot() {
        use nanobpmn_engine_core::Command;

        use crate::readstore::ReadStore;

        let dir = temp_dir("readmodel-reproject-732");

        // Same setup as the #600 test: compact history into the snapshot so
        // `first_index > 0`, then leave a self-consistent post-compaction tail
        // holding a live instance (its service task => a live job).
        {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot+rotate");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(compact(&shared, covered), 1, "sealed prefix compacted away");
            // Post-compaction tail: a second live instance.
            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
        }

        // Reopen: the reconstructed engine (snapshot + surviving tail) is the
        // authoritative live state; the surviving tail sits above a non-zero floor.
        let (engine, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("reopen segmented");
        assert!(recovery.first_index > 0, "must reproduce a non-zero floor");
        let surviving: Vec<&Event> = recovery.events.iter().collect();
        let live_instances = engine.engine_state().instances.len();
        let live_jobs = engine.engine_state().jobs.len();
        assert!(
            live_instances >= 2 && live_jobs >= 2,
            "the engine must hold the live instances/jobs to reproject"
        );

        // A freshly wiped read model (exported_position == 0, below the floor).
        let store = ReadStore::open(None).expect("fresh in-memory read store");
        assert_eq!(store.exported_position(), 0);

        // Default path (no env flag): reproject from the engine snapshot. No panic.
        catch_up_shard(
            &store,
            recovery.first_index,
            &surviving,
            Some(engine.engine_state()),
        );

        assert_eq!(
            store.exported_position() as u64,
            recovery.total_events,
            "reprojection must plant the absolute cursor at total_events"
        );
        assert_eq!(
            store.process_instances().len(),
            live_instances,
            "every live process instance must be recovered from the snapshot"
        );
        assert_eq!(
            store.jobs().len(),
            live_jobs,
            "every live job must be recovered from the snapshot"
        );

        // Regression guard: the planted cursor resumes cleanly on the next boot.
        assert_eq!(
            plan_catch_up(
                store.exported_position() as u64,
                recovery.first_index,
                surviving.len() as u64,
            ),
            CatchUpPlan::Resume {
                skip: surviving.len()
            },
            "the reprojected cursor must resume, not re-abort, on the next boot"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    #[test]
    fn reprojection_restores_terminal_history_from_durable_archive() {
        // Issue #831 end-to-end at the reprojection entrypoint: a terminal instance
        // is archived durably when it completes, and a below-floor reprojection
        // (whose engine snapshot holds only live state) restores that completed
        // history from the archive rather than losing it (the merlin.local defect).
        use crate::readstore::ReadStore;

        let dir = temp_dir("readmodel-archive-reproject-831");
        std::fs::create_dir_all(&dir).expect("create temp dir");
        let store_path = dir.join("read-model.sqlite");
        let store = ReadStore::open(Some(&store_path)).expect("file-backed read store");

        // Complete an instance so `export` writes it to the durable archive.
        let created = Event::ProcessInstanceCreated {
            instance_key: 999,
            process_id: "demo".to_string(),
            variables: std::collections::HashMap::new(),
            created_at: 0,
            tags: Vec::new(),
            business_id: None,
            process_definition_key: 0,
            version: 0,
            parent_process_instance_key: None,
            parent_element_instance_key: None,
        };
        let done = Event::ProcessInstanceCompleted { instance_key: 999 };
        store
            .export(&[&created, &done])
            .expect("export terminal instance");
        assert_eq!(
            store.process_instance(999).map(|r| r.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Completed)
        );

        // Wipe the read model (stand-in for the fingerprint/upgrade wipe) so it
        // sits below the compaction floor, exactly the CompactedGap condition.
        store.reset().expect("wipe read model");
        assert!(store.process_instance(999).is_none());
        assert_eq!(store.exported_position(), 0);

        // The engine snapshot for the reprojection holds NO live instances (the
        // terminal one was long evicted), so only the archive can restore it.
        let empty_dir = temp_dir("readmodel-archive-reproject-831-engine");
        let (engine, _recovery) =
            crate::journal::Journal::open_segmented(&empty_dir).expect("open empty engine");
        assert!(engine.engine_state().instances.is_empty());

        // Drive the public reprojection entrypoint with a non-zero floor.
        catch_up_shard(&store, 100, &[], Some(engine.engine_state()));

        assert_eq!(
            store.process_instance(999).map(|r| r.state),
            Some(nanobpmn_engine_core::ProcessInstanceState::Completed),
            "terminal/completed history must be restored from the durable archive \
             during a below-floor reprojection (issue #831)"
        );

        let _ = fs::remove_dir_all(&dir);
        let _ = fs::remove_dir_all(&empty_dir);
    }

    // ------------------------------------------------------------------------
    // Versioned self-describing snapshot envelope (L2 / #1068).
    // ------------------------------------------------------------------------

    /// Builds a small, deploy-only engine snapshot for envelope tests.
    fn demo_snapshot() -> EngineSnapshot {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(demo()))
            .expect("deploy");
        engine.snapshot()
    }

    fn snap_path(dir: &Path, covered: u64) -> PathBuf {
        dir.join(format!("{SNAP_PREFIX}{covered:020}{SNAP_SUFFIX}"))
    }

    /// Extracts the typed [`SnapshotLoadError`] from a loader's [`io::Error`].
    fn typed_err(e: &io::Error) -> &SnapshotLoadError {
        e.get_ref()
            .and_then(|e| e.downcast_ref::<SnapshotLoadError>())
            .expect("loader error must carry a typed SnapshotLoadError")
    }

    /// GREEN: a snapshot round-trips through the envelope, and the header stamps
    /// the current format version, a non-zero incarnation, and a build
    /// fingerprint — all readable without touching the payload body.
    #[test]
    fn envelope_round_trips_and_stamps_header() {
        let dir = temp_dir("envelope-roundtrip");
        fs::create_dir_all(&dir).unwrap();
        let covered = 7;
        write_snapshot(&dir, demo_snapshot(), covered).expect("write");

        // The on-disk file is `header-line \n payload`.
        let bytes = fs::read(snap_path(&dir, covered)).unwrap();
        let (header, payload) = parse_envelope(&bytes).expect("parse header");
        assert_eq!(header.format_version, SNAPSHOT_FORMAT_VERSION);
        assert_ne!(header.incarnation, 0, "incarnation must be populated");
        assert!(
            header
                .engine_fingerprint
                .starts_with("nanobpmn-engine-core@"),
            "fingerprint = {}",
            header.engine_fingerprint
        );
        // The payload after the header is the real PersistedSnapshot.
        let _: PersistedSnapshot = serde_json::from_slice(payload).expect("payload decodes");

        // The loader returns the engine plus `covered_events` as the 2ND tuple
        // element (caller contract).
        let (loaded, loaded_covered) = load_latest_snapshot(&dir)
            .expect("load ok")
            .expect("snapshot present");
        assert_eq!(loaded_covered, covered);
        assert_eq!(
            loaded.state.processes.len(),
            1,
            "round-tripped snapshot carries the deployed process"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// `Ok(None)` is returned ONLY when no snapshot file exists.
    #[test]
    fn absent_snapshot_is_ok_none() {
        let dir = temp_dir("envelope-absent");
        fs::create_dir_all(&dir).unwrap();
        assert!(load_latest_snapshot(&dir).expect("ok").is_none());
        assert!(load_multi_snapshot(&dir).expect("ok").is_none());
        assert!(
            peek_snapshot_format_version(&snap_path(&dir, 1))
                .expect("ok")
                .is_none()
        );
        let _ = fs::remove_dir_all(&dir);
    }

    /// The format version is readable even when the PAYLOAD is corrupt, and a
    /// corrupt payload is a FATAL typed error — never a silent `None`.
    #[test]
    fn version_readable_from_corrupt_payload_and_load_is_fatal() {
        let dir = temp_dir("envelope-corrupt");
        fs::create_dir_all(&dir).unwrap();
        let covered = 3;
        let header = SnapshotHeader {
            format_version: SNAPSHOT_FORMAT_VERSION,
            incarnation: 42,
            engine_fingerprint: "test".into(),
        };
        let mut file = serde_json::to_vec(&header).unwrap();
        file.push(b'\n');
        file.extend_from_slice(b"this is not valid snapshot json }{");
        fs::write(snap_path(&dir, covered), &file).unwrap();

        // Version legible without decoding the (garbage) payload.
        assert_eq!(
            peek_snapshot_format_version(&snap_path(&dir, covered)).unwrap(),
            Some(SNAPSHOT_FORMAT_VERSION)
        );

        // Loading it is a fatal typed Corrupt error, NOT Ok(None).
        let err = load_latest_snapshot(&dir).expect_err("must be fatal, not None");
        assert!(matches!(typed_err(&err), SnapshotLoadError::Corrupt { .. }));

        let _ = fs::remove_dir_all(&dir);
    }

    /// A snapshot written at a version NEWER than this build supports yields the
    /// typed `SnapshotFormatMismatch` (`FormatMismatch`), never a silent rewind.
    #[test]
    fn future_version_yields_format_mismatch() {
        let dir = temp_dir("envelope-future");
        fs::create_dir_all(&dir).unwrap();
        let covered = 5;
        let future = SNAPSHOT_FORMAT_VERSION + 1;
        let header = SnapshotHeader {
            format_version: future,
            incarnation: 1,
            engine_fingerprint: "future-build".into(),
        };
        // A perfectly VALID payload — the mismatch must trip on the header alone.
        let payload = PersistedSnapshot {
            covered_events: covered,
            engine: demo_snapshot(),
        };
        let mut file = serde_json::to_vec(&header).unwrap();
        file.push(b'\n');
        file.extend_from_slice(&serde_json::to_vec(&payload).unwrap());
        fs::write(snap_path(&dir, covered), &file).unwrap();

        let err = load_latest_snapshot(&dir).expect_err("mismatch is fatal");
        match typed_err(&err) {
            SnapshotLoadError::FormatMismatch { found, supported } => {
                assert_eq!(*found, future);
                assert_eq!(*supported, SNAPSHOT_FORMAT_VERSION);
            }
            other => panic!("expected FormatMismatch, got {other:?}"),
        }
        // The version is still legible.
        assert_eq!(
            peek_snapshot_format_version(&snap_path(&dir, covered)).unwrap(),
            Some(future)
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// Rewrites the snapshot file at `path` so its envelope header advertises an
    /// INCOMPATIBLE (future) format version, leaving the payload bytes untouched.
    /// The next boot then sees a typed `FormatMismatch` from the loader — the
    /// #1071 migration trigger — without any real corruption.
    fn bump_snapshot_header_to_future(path: &Path) {
        let bytes = fs::read(path).expect("snapshot exists");
        let nl = bytes.iter().position(|&b| b == b'\n').expect("header line");
        let payload = &bytes[nl + 1..];
        let header = SnapshotHeader {
            format_version: SNAPSHOT_FORMAT_VERSION + 1,
            incarnation: 99,
            engine_fingerprint: "future-build".into(),
        };
        let mut out = serde_json::to_vec(&header).unwrap();
        out.push(b'\n');
        out.extend_from_slice(payload);
        fs::write(path, &out).expect("rewrite header");
    }

    /// [`bump_snapshot_header_to_future`] for the single-partition snapshot
    /// covering `covered`.
    fn bump_snapshot_to_future(dir: &Path, covered: u64) {
        bump_snapshot_header_to_future(&snap_path(dir, covered));
    }

    /// L5: compaction ARCHIVES the covered prefix into the cold store instead of
    /// hard-deleting it, and `prune_cold_archive` keeps only the most-recent
    /// generation — so the cold store stays bounded to ~one snapshot generation
    /// (the journal analog of the read model's durable terminal archive).
    #[test]
    fn compaction_archives_cold_prefix_and_prune_bounds_it() {
        let dir = temp_dir("cold-archive-bounds");

        let (mut journal, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("open segmented");
        let shared = Arc::clone(&recovery.shared);

        // Generation 1: deploy + instance, snapshot, compact -> archived, not gone.
        let _ = journal
            .apply_command(Command::DeployProcess(demo()))
            .unwrap();
        let _ = journal
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        let (snap1, covered1) = journal.snapshot_and_rotate().expect("snapshot 1");
        write_snapshot(&dir, snap1, covered1).expect("write snapshot 1");
        assert_eq!(compact(&shared, covered1), 1, "sealed prefix compacted");
        assert!(list_sealed(&dir).unwrap().is_empty(), "hot segment removed");

        // The covered prefix survives in the cold store, spanning [0, covered1).
        assert_eq!(
            cold_archive_span(&dir).unwrap(),
            Some((0, covered1)),
            "compaction archived the covered prefix instead of deleting it"
        );

        // Generation 2: another instance, snapshot, compact -> a second cold gen.
        let _ = journal
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        let (snap2, covered2) = journal.snapshot_and_rotate().expect("snapshot 2");
        write_snapshot(&dir, snap2, covered2).expect("write snapshot 2");
        assert_eq!(compact(&shared, covered2), 1);
        assert!(covered2 > covered1);
        assert_eq!(
            cold_archive_span(&dir).unwrap(),
            Some((0, covered2)),
            "both generations are archived before pruning"
        );

        // After a fresh SAME-format snapshot at covered2, prune one generation
        // back (to covered1): the first generation goes, the latest is retained —
        // a bounded rolling window (~one generation of events).
        let removed = prune_cold_archive(&dir, covered1);
        assert_eq!(removed, 1, "the superseded generation is pruned");
        assert_eq!(
            cold_archive_span(&dir).unwrap(),
            Some((covered1, covered2)),
            "the cold store stays bounded to the latest generation"
        );

        drop(shared);
        // Normal recovery is unaffected (the snapshot is in-format).
        let (reopened, recovery) = crate::journal::Journal::open_segmented(&dir).expect("reopen");
        assert!(!recovery.fresh);
        assert_eq!(reopened.state().processes.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    /// #1076 regression: the multi-partition maintenance loop's cold-archive prune
    /// floor must derive from the covered watermarks of the partitions THIS node
    /// OWNS, not from a global-width vector padded with 0 for unowned partitions. A
    /// clustered node owns only a subset of partitions, so a global `covered` has
    /// 0-holes; the old `covered.iter().min()` pinned the floor at 0 forever and
    /// `prune_cold_archive` never pruned — an unbounded cold archive.
    #[test]
    fn cold_prune_floor_uses_owned_partitions_only() {
        // Clustered node owns partitions 0 and 2 of a 4-partition cluster and
        // captured covered watermarks 30 and 50 this tick. The floor is 30 (min
        // of OWNED), not 0.
        assert_eq!(cold_prune_floor(&[30, 50]), 30);

        // Regression witness: the OLD global-width form min([30, 0, 50, 0]) == 0,
        // which is exactly the floor that never advanced (bug #1076).
        assert_eq!([30u64, 0, 50, 0].iter().copied().min().unwrap(), 0);

        // Single node owning every partition: identical to the global min.
        assert_eq!(cold_prune_floor(&[42, 42, 42, 42]), 42);

        // A tick that snapshotted nothing yields floor 0 (prune no-op).
        assert_eq!(cold_prune_floor(&[]), 0);
    }

    /// L5 core regression for #1065 (`instance 41` rewind): a snapshot written in
    /// an INCOMPATIBLE format is transparently migrated by REPLAYING the journal
    /// (cold archive prefix + surviving hot tail) under the new code on the next
    /// boot — engine state, definitions, AND the key high-water are preserved, so
    /// a freshly created instance never collides with a pre-migration key.
    #[test]
    fn incompatible_snapshot_migrates_by_replay_preserving_key_high_water() {
        let dir = temp_dir("migrate-replay");

        let (key1, key2) = {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);

            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let (e1, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key1 = e1.iter().find_map(|e| e.instance_key()).unwrap();

            // Snapshot + compact so key1's events live ONLY in the cold archive.
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(compact(&shared, covered), 1);
            assert_eq!(cold_archive_span(&dir).unwrap(), Some((0, covered)));

            // key2 lands in the surviving hot tail (after the cold prefix).
            let (e2, commit) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key2 = e2.iter().find_map(|e| e.instance_key()).unwrap();
            commit.blocking_wait();

            // The persisted snapshot is now unreadable (incompatible version).
            bump_snapshot_to_future(&dir, covered);
            (key1, key2)
        };

        // Boot: the mismatch triggers replay-migration (cold prefix + hot tail).
        let (mut reopened, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("migrating reopen");
        assert!(!recovery.fresh);
        assert!(
            reopened.instance(key1).is_some(),
            "the compacted (cold-archived) instance is rebuilt by replay"
        );
        assert!(
            reopened.instance(key2).is_some(),
            "the hot-tail instance is preserved across migration"
        );
        assert_eq!(reopened.state().processes.len(), 1, "definitions preserved");

        // KEY HIGH-WATER: a new instance must advance PAST every pre-migration
        // key — the exact #1065 `instance 41` rewind guard.
        let (e3, _) = reopened
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        let key3 = e3.iter().find_map(|e| e.instance_key()).unwrap();
        use nanobpmn_engine_core::local_of;
        assert!(
            local_of(key3) > local_of(key2) && local_of(key3) > local_of(key1),
            "post-migration key {key3} must not rewind onto a used key ({key1}, {key2})"
        );

        // A fresh in-format snapshot was written and is loadable again.
        assert!(
            load_latest_snapshot(&dir).expect("in-format now").is_some(),
            "migration rewrote a loadable NEW-format snapshot"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// #1065 guard (no-snapshot variant): when the snapshot file is ABSENT but
    /// compaction advanced the journal past 0, boot must reconstruct the full
    /// history from the cold archive + surviving tail — NOT replay only the tail
    /// and silently rewind everything below `first_index`.
    #[test]
    fn no_snapshot_over_compacted_journal_reconstructs_from_cold() {
        let dir = temp_dir("no-snap-reconstruct");

        let (key1, key2, covered) = {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);

            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let (e1, _) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key1 = e1.iter().find_map(|e| e.instance_key()).unwrap();

            // Snapshot + compact so key1 lives ONLY in the cold archive.
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(compact(&shared, covered), 1);
            assert_eq!(cold_archive_span(&dir).unwrap(), Some((0, covered)));

            // key2 lands in the surviving hot tail (first_index == covered > 0).
            let (e2, commit) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let key2 = e2.iter().find_map(|e| e.instance_key()).unwrap();
            commit.blocking_wait();

            // The snapshot is GONE (lost/never-flushed) while the journal stayed
            // compacted — the exact partial-loss shape that must not rewind.
            fs::remove_file(snap_path(&dir, covered)).expect("remove snapshot");
            assert!(load_latest_snapshot(&dir).expect("ok").is_none());
            (key1, key2, covered)
        };

        let (mut reopened, recovery) =
            crate::journal::Journal::open_segmented(&dir).expect("reconstructing reopen");
        assert!(!recovery.fresh);
        assert!(
            reopened.instance(key1).is_some(),
            "the compacted (cold-archived) instance is rebuilt, not rewound"
        );
        assert!(
            reopened.instance(key2).is_some(),
            "the hot-tail instance is preserved"
        );

        // No key rewind: a fresh instance advances past every prior key.
        let (e3, _) = reopened
            .apply_command(Command::create_instance("demo"))
            .unwrap();
        let key3 = e3.iter().find_map(|e| e.instance_key()).unwrap();
        use nanobpmn_engine_core::local_of;
        assert!(
            local_of(key3) > local_of(key2) && local_of(key3) > local_of(key1),
            "post-reconstruction key {key3} must not rewind onto ({key1}, {key2})"
        );

        // Reconstruction rewrote a fresh loadable snapshot covering the history.
        assert!(
            load_latest_snapshot(&dir).expect("in-format now").is_some(),
            "reconstruction rewrote a loadable snapshot"
        );
        let _ = covered;
        let _ = fs::remove_dir_all(&dir);
    }

    /// #1066 fail-closed (no-snapshot variant): snapshot ABSENT, the cold prefix
    /// pruned away, and the journal compacted past 0 — the history cannot be
    /// reconstructed, so boot must FAIL rather than replay only the tail and
    /// rewind.
    #[test]
    fn no_snapshot_over_pruned_journal_fails_closed() {
        let dir = temp_dir("no-snap-gap");

        {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);

            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(compact(&shared, covered), 1);

            let (_e, commit) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            commit.blocking_wait();

            // Prune the cold prefix AND drop the snapshot: [0, covered) is gone.
            assert_eq!(prune_cold_archive(&dir, covered), 1);
            assert_eq!(cold_archive_span(&dir).unwrap(), None);
            fs::remove_file(snap_path(&dir, covered)).expect("remove snapshot");
        }

        match recover(&dir) {
            Err(e) => assert_eq!(e.kind(), io::ErrorKind::InvalidData),
            Ok(_) => panic!("must fail closed, never rewind"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// L5 fail-closed guard: when the migration cannot reconstruct the full
    /// history (the cold archive's covered prefix was pruned away, so it survives
    /// only inside the unreadable snapshot), boot fails LOUD rather than replaying
    /// a partial history and rewinding.
    #[test]
    fn migration_fails_closed_on_pruned_gap() {
        let dir = temp_dir("migrate-gap");

        let covered = {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);

            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(compact(&shared, covered), 1);

            // A surviving hot-tail event so the tail starts ABOVE 0.
            let (_e, commit) = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            commit.blocking_wait();

            // Prune the cold prefix away: [0, covered) now lives only in the
            // (about-to-be-unreadable) snapshot — a genuine gap.
            assert_eq!(prune_cold_archive(&dir, covered), 1);
            assert_eq!(cold_archive_span(&dir).unwrap(), None);

            bump_snapshot_to_future(&dir, covered);
            covered
        };

        // Boot must FAIL (typed mismatch) rather than replay [covered, total) and
        // rewind everything below `covered`.
        let err = match recover(&dir) {
            Err(e) => e,
            Ok(_) => panic!("must fail closed, never rewind"),
        };
        assert!(
            matches!(typed_err(&err), SnapshotLoadError::FormatMismatch { .. }),
            "a pruned-gap migration fails closed with the typed snapshot error"
        );
        let _ = covered;

        let _ = fs::remove_dir_all(&dir);
    }

    /// L5 fail-closed guard: when the event journal itself is unreadable (an
    /// unknown-variant frame — the #1070 discipline was violated) the migration
    /// replay surfaces the typed decode error as a hard failure. Both snapshot AND
    /// journal are unreadable, so boot fails loud rather than corrupting.
    #[test]
    fn migration_fails_closed_on_unreadable_event_frame() {
        let dir = temp_dir("migrate-badframe");

        let covered = {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);

            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(compact(&shared, covered), 1);
            assert_eq!(cold_archive_span(&dir).unwrap(), Some((0, covered)));
            covered
        };

        // Corrupt the cold archive's contents to an unknown event variant while
        // keeping its [0, covered) range name (so the contiguity check passes and
        // the replay is attempted, then hits the undecodable frame).
        let (_, _, cold_path) = list_cold(&dir).unwrap().into_iter().next().unwrap();
        let bad = deflate_all(b"{\"NosuchEvent\":{}}\n").unwrap();
        fs::write(&cold_path, &bad).unwrap();

        bump_snapshot_to_future(&dir, covered);

        // Migration replay hits the unknown-variant frame -> hard error (never a
        // silent partial rebuild).
        let err = match recover(&dir) {
            Err(e) => e,
            Ok(_) => panic!("unreadable frame must fail closed"),
        };
        assert_eq!(
            err.kind(),
            io::ErrorKind::InvalidData,
            "the undecodable event frame surfaces as a fatal error"
        );

        let _ = fs::remove_dir_all(&dir);
    }

    /// L5 fail-closed guard: a cold-archive file whose decoded record count does
    /// not match its named `[start, end)` range (a truncated / mis-sized frame) is
    /// a hard failure — `cold_archive_span` alone could not distinguish it from a
    /// sound prefix, so the migrator's per-file count validation is what refuses
    /// to reconstruct from a silently-short history.
    #[test]
    fn migration_fails_closed_on_mis_sized_cold_file() {
        let dir = temp_dir("migrate-shortcold");

        let covered = {
            let (mut journal, recovery) =
                crate::journal::Journal::open_segmented(&dir).expect("open segmented");
            let shared = Arc::clone(&recovery.shared);

            let _ = journal
                .apply_command(Command::DeployProcess(demo()))
                .unwrap();
            let _ = journal
                .apply_command(Command::create_instance("demo"))
                .unwrap();
            let (snap, covered) = journal.snapshot_and_rotate().expect("snapshot");
            write_snapshot(&dir, snap, covered).expect("write snapshot");
            assert_eq!(compact(&shared, covered), 1);
            // A real, multi-record archived prefix.
            assert!(
                covered > 1,
                "need >1 event to drop some and still stay valid"
            );
            covered
        };

        // Rewrite the cold file with only its FIRST (valid) record while keeping
        // its [0, covered) name: every line still DECODES, but the count is short.
        let (_, _, cold_path) = list_cold(&dir).unwrap().into_iter().next().unwrap();
        let full = inflate_all(&fs::read(&cold_path).unwrap()).unwrap();
        let first_line_end = full.iter().position(|&b| b == b'\n').unwrap() + 1;
        let short = deflate_all(&full[..first_line_end]).unwrap();
        fs::write(&cold_path, &short).unwrap();

        bump_snapshot_to_future(&dir, covered);

        // The count mismatch is fatal — never a silent short replay / rewind.
        let err = match recover(&dir) {
            Err(e) => e,
            Ok(_) => panic!("mis-sized cold file must fail closed"),
        };
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        let _ = fs::remove_dir_all(&dir);
    }

    /// L5 multi-partition path: an incompatible COMBINED snapshot is migrated by
    /// replaying every owned partition's full tagged history (cold archive +
    /// surviving tail), demuxed by write tag — engine state and per-partition key
    /// high-water preserved across `compact_multi` + `recover_multi`.
    #[test]
    fn incompatible_multi_snapshot_migrates_by_replay() {
        let dir = temp_dir("migrate-multi");
        let (tx, _rx) = std::sync::mpsc::channel::<crate::journal::ExportBatch>();

        let (key0, key1, covered0, covered1) = {
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[0, 1], 2, None)
                    .expect("open multi");
            let seg = Arc::clone(&recovery.shared);
            let mut engines: std::collections::HashMap<u64, Engine> =
                recovery.engines.into_iter().collect();
            let mut j0 = crate::journal::Journal::from_engine_shared(
                0,
                engines.remove(&0).unwrap(),
                true,
                &writer,
            );
            let mut j1 = crate::journal::Journal::from_engine_shared(
                1,
                engines.remove(&1).unwrap(),
                true,
                &writer,
            );
            j0.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));

            let (deploy_events, _) = j0.apply_command(Command::DeployProcess(demo())).unwrap();
            j1.install_deployment(&deploy_events);

            let (e0, _) = j0.apply_command(Command::create_instance("demo")).unwrap();
            let key0 = e0.iter().find_map(|e| e.instance_key()).unwrap();
            let (e1, c1) = j1.apply_command(Command::create_instance("demo")).unwrap();
            let key1 = e1.iter().find_map(|e| e.instance_key()).unwrap();
            c1.blocking_wait();

            let (snap0, covered0) = j0.snapshot_and_rotate().expect("snapshot p0");
            let (snap1, covered1) = j1.snapshot_and_rotate().expect("snapshot p1");
            write_multi_snapshot(&dir, vec![(0, covered0, snap0), (1, covered1, snap1)])
                .expect("write combined snapshot");

            // Compact: the sealed prefix is archived (not deleted) for every
            // partition, then removed from hot storage.
            let covered = [covered0, covered1];
            assert_eq!(compact_multi(&seg, &covered, &[u64::MAX; 2]), 1);
            assert!(seg.sealed.lock().unwrap().is_empty());
            assert!(
                cold_archive_span(&dir).unwrap().is_some(),
                "compact_multi archived the covered prefix"
            );

            // The combined snapshot is now unreadable at its format version.
            bump_snapshot_header_to_future(&dir.join(MULTI_SNAP_NAME));
            (key0, key1, covered0, covered1)
        };
        let _ = (covered0, covered1);

        // Boot: replay-migrate every owned partition from the cold archive.
        let recovery = recover_multi(&dir, &[0, 1], 2, None).expect("migrating reopen multi");
        assert!(!recovery.fresh);
        let engines: std::collections::HashMap<u64, Engine> =
            recovery.engines.into_iter().collect();
        assert!(
            engines[&0].instance(key0).is_some(),
            "partition 0 rebuilt by replay across migration"
        );
        assert!(
            engines[&1].instance(key1).is_some(),
            "partition 1 rebuilt by replay across migration"
        );
        assert_eq!(engines[&0].state().processes.len(), 1, "definitions kept");

        let _ = fs::remove_dir_all(&dir);
    }

    /// #1065 guard (multi, no-snapshot variant): the combined snapshot is ABSENT
    /// but compaction advanced the journal past 0 — `recover_multi` must rebuild
    /// every owned partition from the cold archive + surviving tail rather than
    /// replaying only each tail and silently rewinding.
    #[test]
    fn no_multi_snapshot_over_compacted_journal_reconstructs_from_cold() {
        let dir = temp_dir("no-msnap-reconstruct");
        let (tx, _rx) = std::sync::mpsc::channel::<crate::journal::ExportBatch>();

        let (key0, key1) = {
            let (writer, recovery) =
                crate::journal::SharedWriter::open_segmented(&dir, &[0, 1], 2, None)
                    .expect("open multi");
            let seg = Arc::clone(&recovery.shared);
            let mut engines: std::collections::HashMap<u64, Engine> =
                recovery.engines.into_iter().collect();
            let mut j0 = crate::journal::Journal::from_engine_shared(
                0,
                engines.remove(&0).unwrap(),
                true,
                &writer,
            );
            let mut j1 = crate::journal::Journal::from_engine_shared(
                1,
                engines.remove(&1).unwrap(),
                true,
                &writer,
            );
            j0.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
            j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));

            let (deploy_events, _) = j0.apply_command(Command::DeployProcess(demo())).unwrap();
            j1.install_deployment(&deploy_events);

            let (e0, _) = j0.apply_command(Command::create_instance("demo")).unwrap();
            let key0 = e0.iter().find_map(|e| e.instance_key()).unwrap();
            let (e1, c1) = j1.apply_command(Command::create_instance("demo")).unwrap();
            let key1 = e1.iter().find_map(|e| e.instance_key()).unwrap();
            c1.blocking_wait();

            let (snap0, covered0) = j0.snapshot_and_rotate().expect("snapshot p0");
            let (snap1, covered1) = j1.snapshot_and_rotate().expect("snapshot p1");
            write_multi_snapshot(&dir, vec![(0, covered0, snap0), (1, covered1, snap1)])
                .expect("write combined snapshot");

            let covered = [covered0, covered1];
            assert_eq!(compact_multi(&seg, &covered, &[u64::MAX; 2]), 1);
            assert!(
                cold_archive_span(&dir).unwrap().is_some(),
                "compact_multi archived the covered prefix"
            );

            // The combined snapshot is GONE while the journal stayed compacted.
            fs::remove_file(dir.join(MULTI_SNAP_NAME)).expect("remove combined snapshot");
            assert!(load_multi_snapshot(&dir).expect("ok").is_none());
            (key0, key1)
        };

        let recovery = recover_multi(&dir, &[0, 1], 2, None).expect("reconstructing reopen multi");
        assert!(!recovery.fresh);
        let engines: std::collections::HashMap<u64, Engine> =
            recovery.engines.into_iter().collect();
        assert!(
            engines[&0].instance(key0).is_some(),
            "partition 0 rebuilt from the cold archive, not rewound"
        );
        assert!(
            engines[&1].instance(key1).is_some(),
            "partition 1 rebuilt from the cold archive, not rewound"
        );
        assert_eq!(engines[&0].state().processes.len(), 1, "definitions kept");

        let _ = fs::remove_dir_all(&dir);
    }

    /// The tagged cold reader fails closed on a corrupt (non-numeric) write tag
    /// rather than silently treating the line as untagged and misrouting the
    /// event to its key partition — the cold archive is authoritative and never
    /// torn, so a malformed tag is fatal, not a fallback.
    #[test]
    fn parse_cold_file_tagged_rejects_non_numeric_tag() {
        let dir = temp_dir("cold-bad-tag");
        fs::create_dir_all(&dir).unwrap();
        let path = cold_name(&dir, 0, 1);
        let plaintext = b"xx\t{\"DeploymentCreated\":{\"deployment_key\":1}}\n".to_vec();
        fs::write(&path, deflate_all(&plaintext).unwrap()).unwrap();

        let err = parse_cold_file_tagged(&path, 2)
            .expect_err("a non-numeric write tag must be rejected, not defaulted");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let _ = fs::remove_dir_all(&dir);
    }

    /// Legacy headerless snapshots (bare `serde_json`, no envelope) still load,
    /// are treated as `format_version = 0`, and keep `covered_events` as the 2nd
    /// tuple element.
    #[test]
    fn legacy_headerless_snapshot_still_loads() {
        let dir = temp_dir("envelope-legacy");
        fs::create_dir_all(&dir).unwrap();
        let covered = 9;
        // Exactly the OLD on-disk format: bare compact JSON, no header line.
        let payload = PersistedSnapshot {
            covered_events: covered,
            engine: demo_snapshot(),
        };
        fs::write(
            snap_path(&dir, covered),
            serde_json::to_vec(&payload).unwrap(),
        )
        .unwrap();

        assert_eq!(
            peek_snapshot_format_version(&snap_path(&dir, covered)).unwrap(),
            Some(0),
            "headerless legacy file reads as format_version 0"
        );
        let (loaded, loaded_covered) = load_latest_snapshot(&dir)
            .expect("legacy load ok")
            .expect("present");
        assert_eq!(loaded_covered, covered);
        assert_eq!(loaded.state.processes.len(), 1);

        let _ = fs::remove_dir_all(&dir);
    }

    /// The incarnation id is generated once per data dir and stays stable across
    /// successive snapshot writes (so a consumer can detect a reset/restore by a
    /// CHANGE in it).
    #[test]
    fn incarnation_is_stable_across_writes() {
        let dir = temp_dir("envelope-incarnation");
        fs::create_dir_all(&dir).unwrap();
        write_snapshot(&dir, demo_snapshot(), 1).unwrap();
        let first = parse_envelope(&fs::read(snap_path(&dir, 1)).unwrap())
            .unwrap()
            .0
            .incarnation;
        write_snapshot(&dir, demo_snapshot(), 2).unwrap();
        let second = parse_envelope(&fs::read(snap_path(&dir, 2)).unwrap())
            .unwrap()
            .0
            .incarnation;
        assert_eq!(first, second, "incarnation is stable for one data dir");
        assert_ne!(first, 0);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A corrupt/zero incarnation file must NOT be silently regenerated (which
    /// would spuriously mutate the durable identity and surface a false
    /// reset/restore to nano-workforce#622). It fails closed as `InvalidData`,
    /// leaving the on-disk file untouched. Only a genuinely *missing* file seeds
    /// a fresh incarnation.
    #[test]
    fn corrupt_incarnation_file_is_rejected_not_regenerated() {
        let dir = temp_dir("incarnation-corrupt");
        fs::create_dir_all(&dir).unwrap();

        // Non-numeric contents -> InvalidData, file left untouched.
        fs::write(dir.join(INCARNATION_NAME), b"not-a-number").unwrap();
        let err = read_or_init_incarnation(&dir).expect_err("non-numeric rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        assert_eq!(
            fs::read_to_string(dir.join(INCARNATION_NAME)).unwrap(),
            "not-a-number",
            "corrupt file must not be overwritten"
        );

        // Reserved zero -> InvalidData.
        fs::write(dir.join(INCARNATION_NAME), b"0").unwrap();
        let err = read_or_init_incarnation(&dir).expect_err("zero rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);

        // Genuinely missing -> seeds a fresh non-zero incarnation.
        fs::remove_file(dir.join(INCARNATION_NAME)).unwrap();
        let v = read_or_init_incarnation(&dir).expect("missing file seeds fresh id");
        assert_ne!(v, 0);
        // And is now stable.
        assert_eq!(read_or_init_incarnation(&dir).unwrap(), v);

        let _ = fs::remove_dir_all(&dir);
    }

    /// The multi-partition combined snapshot round-trips through the same
    /// envelope, and a future header version is likewise a fatal mismatch.
    #[test]
    fn multi_envelope_round_trips_and_rejects_future_version() {
        let dir = temp_dir("envelope-multi");
        fs::create_dir_all(&dir).unwrap();
        write_multi_snapshot(&dir, vec![(0, 4, demo_snapshot()), (1, 6, demo_snapshot())])
            .expect("write multi");

        let map = load_multi_snapshot(&dir)
            .expect("load ok")
            .expect("present");
        assert_eq!(map.len(), 2);
        assert_eq!(map.get(&0).unwrap().0, 4);
        assert_eq!(map.get(&1).unwrap().0, 6);
        assert_eq!(
            peek_snapshot_format_version(&dir.join(MULTI_SNAP_NAME)).unwrap(),
            Some(SNAPSHOT_FORMAT_VERSION)
        );

        // Overwrite with a future-version header + valid payload.
        let header = SnapshotHeader {
            format_version: SNAPSHOT_FORMAT_VERSION + 1,
            incarnation: 1,
            engine_fingerprint: "future".into(),
        };
        let payload = MultiPersistedSnapshot {
            entries: vec![MultiSnapshotEntry {
                partition: 0,
                covered: 4,
                engine: demo_snapshot(),
            }],
        };
        let mut file = serde_json::to_vec(&header).unwrap();
        file.push(b'\n');
        file.extend_from_slice(&serde_json::to_vec(&payload).unwrap());
        fs::write(dir.join(MULTI_SNAP_NAME), &file).unwrap();

        let err = load_multi_snapshot(&dir).expect_err("mismatch is fatal");
        assert!(matches!(
            typed_err(&err),
            SnapshotLoadError::FormatMismatch { .. }
        ));

        let _ = fs::remove_dir_all(&dir);
    }

    /// A legacy headerless combined snapshot still loads as `format_version = 0`.
    #[test]
    fn legacy_headerless_multi_snapshot_still_loads() {
        let dir = temp_dir("envelope-multi-legacy");
        fs::create_dir_all(&dir).unwrap();
        let payload = MultiPersistedSnapshot {
            entries: vec![MultiSnapshotEntry {
                partition: 2,
                covered: 8,
                engine: demo_snapshot(),
            }],
        };
        fs::write(
            dir.join(MULTI_SNAP_NAME),
            serde_json::to_vec(&payload).unwrap(),
        )
        .unwrap();

        let map = load_multi_snapshot(&dir)
            .expect("legacy multi load ok")
            .expect("present");
        assert_eq!(map.get(&2).unwrap().0, 8);
        let _ = fs::remove_dir_all(&dir);
    }

    /// A journal whose records all name known variants replays cleanly through
    /// the typed decode path (regression floor for the tests below).
    #[test]
    fn read_segment_events_decodes_a_valid_plaintext_journal() {
        let dir = temp_dir("valid-journal");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("seg.jsonl");
        fs::write(
            &path,
            "{\"DeploymentCreated\":{\"deployment_key\":1}}\n\
             {\"ProcessInstanceCompleted\":{\"instance_key\":2}}\n",
        )
        .unwrap();

        let events = read_segment_events(&path).expect("valid journal reads");
        assert_eq!(events.len(), 2);
        let _ = fs::remove_dir_all(&dir);
    }

    /// An unknown/removed event variant in a journal must surface as the typed,
    /// downcastable [`EventDecodeError::UnknownVariant`] on read — NOT a silent
    /// skip and NOT an anonymous error — so #1071's replay-migrator (or the
    /// fail-closed path, #1066) can branch on it. This is the storage-boundary
    /// half of L4 (#1070); the engine-core `golden_replay` tests cover the
    /// decoder itself.
    ///
    /// Critically, the unknown record is placed as the **last** content line to
    /// prove it is fatal even where a genuinely torn (truncated) tail would be
    /// tolerated: a complete, well-formed unknown frame is never a torn write.
    #[test]
    fn read_segment_events_rejects_unknown_variant_as_typed_error() {
        let dir = temp_dir("unknown-variant");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("seg.jsonl");
        fs::write(
            &path,
            "{\"DeploymentCreated\":{\"deployment_key\":1}}\n\
             {\"NoSuchEventFromTheFuture\":{\"instance_key\":9}}\n",
        )
        .unwrap();

        let err = read_segment_events(&path).expect_err("unknown variant must be rejected");
        assert_eq!(err.kind(), io::ErrorKind::InvalidData);
        let decode_err = err
            .get_ref()
            .and_then(|e| e.downcast_ref::<EventDecodeError>())
            .expect("error downcasts to the typed EventDecodeError");
        match decode_err {
            EventDecodeError::UnknownVariant { variant, .. } => {
                assert_eq!(variant, "NoSuchEventFromTheFuture")
            }
            other => panic!("expected UnknownVariant, got {other:?}"),
        }
        let _ = fs::remove_dir_all(&dir);
    }

    /// The tagged multi-partition reader applies the same discipline: an unknown
    /// variant (here on a `<partition>\t<json>` line) is a typed rejection, not a
    /// dropped record.
    #[test]
    fn read_segment_events_tagged_rejects_unknown_variant() {
        let dir = temp_dir("unknown-variant-tagged");
        fs::create_dir_all(&dir).unwrap();
        let path = dir.join("seg.jsonl");
        fs::write(
            &path,
            "0\t{\"DeploymentCreated\":{\"deployment_key\":1}}\n\
             0\t{\"RetiredLegacyEvent\":{\"instance_key\":9}}\n",
        )
        .unwrap();

        let err =
            read_segment_events_tagged(&path, 1).expect_err("unknown variant must be rejected");
        let decode_err = err
            .get_ref()
            .and_then(|e| e.downcast_ref::<EventDecodeError>())
            .expect("error downcasts to EventDecodeError");
        assert!(matches!(
            decode_err,
            EventDecodeError::UnknownVariant { .. }
        ));
        let _ = fs::remove_dir_all(&dir);
    }

    // ------------------------------------------------------------------------
    // Formal-spec conformance anchor (#1229).
    //
    // These tests anchor the TLA+ model `formal/tla/snapshot/SnapshotReplay.tla`
    // to the real recovery+migration path in THIS module, so the spec cannot
    // silently drift from the implementation it claims to model. The spec models
    // recovery as a pure function `RecoverOutcome` of the durable on-disk world
    // and model-checks two safety invariants:
    //
    //   * NoSilentRewind (#1065) — whenever recovery SUCCEEDS it reconstructs the
    //     FULL `[0, total_events)` history; it never silently rebuilds a
    //     strictly-older (shorter) prefix (here: never loses a prior instance and
    //     never rewinds the key generator).
    //   * FailClosed (#1066) — when the full history genuinely cannot be
    //     reconstructed (a pruned gap, or an unreadable / `UnknownVariant` frame
    //     in a source it must read), recovery REJECTS with a typed error rather
    //     than silently proceeding on a partial history.
    //
    // Every scenario below is one durable-world class the spec's `RecoverOutcome`
    // maps to either `Rebuilt(total)` (Expect::ReconstructsFull) or `Reject`
    // (Expect::Rejects). The `assert_recovery` helper enforces exactly the two
    // invariants above against the real `Journal::open_segmented` → `recover`
    // path. This mirrors the engine-core trace-validation pattern (#1226) for the
    // storage subsystem, which that pattern does not cover.
    mod snapshot_replay_conformance {
        use std::{fs, io, path::Path};

        use nanobpmn_engine_core::{Command, local_of};

        use super::super::{cold_archive_span, deflate_all, list_cold, prune_cold_archive};
        use super::{bump_snapshot_to_future, compacted_dir_with_two_instances, snapshot_file};
        use crate::journal::Journal;

        /// The terminal outcome the spec's `RecoverOutcome` predicts for a world.
        enum Expect {
            /// `Rebuilt(total)` — the full history is reconstructed (NoSilentRewind).
            ReconstructsFull,
            /// `Reject` — recovery fails closed with a typed error (FailClosed).
            Rejects,
        }

        /// Runs the REAL recovery path over `dir` and asserts the SnapshotReplay
        /// spec's two safety invariants for the declared expected outcome.
        fn assert_recovery(dir: &Path, priors: &[u64], expect: Expect) {
            match Journal::open_segmented(dir) {
                Ok((mut journal, recovery)) => {
                    assert!(
                        matches!(expect, Expect::ReconstructsFull),
                        "FailClosed violated: recovery silently proceeded on a world the spec \
                         rejects (dir {})",
                        dir.display()
                    );
                    assert!(
                        !recovery.fresh,
                        "recovered durable state must not read as fresh"
                    );
                    // NoSilentRewind: every prior instance survives (no strictly-older history).
                    for &key in priors {
                        assert!(
                            journal.instance(key).is_some(),
                            "NoSilentRewind violated: recovery lost prior instance {key}"
                        );
                    }
                    // NoSilentRewind covers non-instance durable state too: the
                    // deployed process definition (a `DeploymentCreated`/process
                    // record, not an instance row) must survive recovery, so a
                    // regression that drops non-instance history is caught here and
                    // not only by the instance checks above.
                    assert!(
                        !journal.state().processes.is_empty(),
                        "NoSilentRewind violated: recovery lost the deployed process \
                         definition (dir {})",
                        dir.display()
                    );
                    // NoSilentRewind: the key generator is not rewound — a fresh instance
                    // advances strictly past every prior key.
                    let (events, _) = journal
                        .apply_command(Command::create_instance("demo"))
                        .expect("post-recovery instance creation");
                    let fresh = events
                        .iter()
                        .find_map(|e| e.instance_key())
                        .expect("a created instance has a key");
                    let max_prior = priors.iter().map(|&k| local_of(k)).max().unwrap_or(0);
                    assert!(
                        local_of(fresh) > max_prior,
                        "NoSilentRewind violated: key generator rewound (fresh {} <= max prior {})",
                        local_of(fresh),
                        max_prior
                    );
                }
                Err(e) => {
                    assert!(
                        matches!(expect, Expect::Rejects),
                        "recovery rejected a world the spec reconstructs (dir {}): {e}",
                        dir.display()
                    );
                    // FailClosed is a typed, loud reject — never a silent success/rewind.
                    assert_eq!(
                        e.kind(),
                        io::ErrorKind::InvalidData,
                        "fail-closed recovery must surface a typed InvalidData reject: {e}"
                    );
                }
            }
        }

        /// The cold-archive end (== compaction floor == snapshot `covered`) of a
        /// `compacted_dir_with_two_instances` world.
        fn covered(dir: &Path) -> u64 {
            cold_archive_span(dir)
                .expect("list cold archive")
                .expect("a compacted world has a cold archive")
                .1
        }

        /// Spec world: readable, non-stale snapshot present (`SnapReadable`).
        /// `RecoverOutcome = Rebuilt(total)` via snapshot base + hot tail.
        #[test]
        fn readable_snapshot_reconstructs_full() {
            let (dir, key1, key2) = compacted_dir_with_two_instances("conf-readable");
            assert_recovery(&dir, &[key1, key2], Expect::ReconstructsFull);
            let _ = fs::remove_dir_all(&dir);
        }

        /// Spec world: snapshot present but unreadable (a newer/incompatible
        /// `format_version`), cold prefix meets the floor and decodes. The
        /// migrator replays cold + hot: `RecoverOutcome = Rebuilt(total)` (#1071).
        #[test]
        fn unreadable_snapshot_migrates_from_cold() {
            let (dir, key1, key2) = compacted_dir_with_two_instances("conf-migrate");
            bump_snapshot_to_future(&dir, covered(&dir));
            assert_recovery(&dir, &[key1, key2], Expect::ReconstructsFull);
            let _ = fs::remove_dir_all(&dir);
        }

        /// Spec world: no snapshot (`~snapPresent`), floor > 0, cold prefix meets
        /// the floor. The migrator reconstructs from cold + hot:
        /// `RecoverOutcome = Rebuilt(total)`.
        #[test]
        fn lost_snapshot_reconstructs_from_cold() {
            let (dir, key1, key2) = compacted_dir_with_two_instances("conf-lost-snap");
            fs::remove_file(snapshot_file(&dir)).expect("drop the snapshot");
            assert_recovery(&dir, &[key1, key2], Expect::ReconstructsFull);
            let _ = fs::remove_dir_all(&dir);
        }

        /// Spec world: no snapshot AND the cold prefix pruned below the floor
        /// (`coldEnd < firstIndex`) — a genuine gap in `[0, total)`.
        /// `RecoverOutcome = Reject` (FailClosed, #1066).
        #[test]
        fn pruned_gap_no_snapshot_rejects() {
            let (dir, _key1, _key2) = compacted_dir_with_two_instances("conf-gap-nosnap");
            let c = covered(&dir);
            fs::remove_file(snapshot_file(&dir)).expect("drop the snapshot");
            assert_eq!(
                prune_cold_archive(&dir, c),
                1,
                "the cold prefix is pruned away"
            );
            assert!(
                cold_archive_span(&dir).unwrap().is_none(),
                "the gap is real"
            );
            assert_recovery(&dir, &[], Expect::Rejects);
            let _ = fs::remove_dir_all(&dir);
        }

        /// Spec world: snapshot unreadable AND the cold prefix pruned below the
        /// floor — the compacted prefix survives only inside the unreadable
        /// snapshot. `RecoverOutcome = Reject` (FailClosed, #1066).
        #[test]
        fn pruned_gap_unreadable_snapshot_rejects() {
            let (dir, _key1, _key2) = compacted_dir_with_two_instances("conf-gap-badsnap");
            let c = covered(&dir);
            assert_eq!(
                prune_cold_archive(&dir, c),
                1,
                "the cold prefix is pruned away"
            );
            bump_snapshot_to_future(&dir, c);
            assert_recovery(&dir, &[], Expect::Rejects);
            let _ = fs::remove_dir_all(&dir);
        }

        /// Spec world: snapshot unreadable, cold prefix meets the floor but no
        /// longer decodes (an `UnknownVariant` frame — the #1070 discipline was
        /// violated). The migrator replay hits the undecodable frame:
        /// `RecoverOutcome = Reject` (FailClosed, #1066/#1070).
        #[test]
        fn unreadable_cold_frame_rejects() {
            let (dir, _key1, _key2) = compacted_dir_with_two_instances("conf-badframe");
            let c = covered(&dir);
            // Corrupt the cold contents to an unknown event variant while keeping
            // the file's [0, covered) range name (so the contiguity check passes
            // and the replay is attempted, then hits the undecodable frame).
            let (_, _, cold_path) = list_cold(&dir).unwrap().into_iter().next().unwrap();
            let bad = deflate_all(b"{\"NosuchEvent\":{}}\n").unwrap();
            fs::write(&cold_path, &bad).unwrap();
            bump_snapshot_to_future(&dir, c);
            assert_recovery(&dir, &[], Expect::Rejects);
            let _ = fs::remove_dir_all(&dir);
        }

        /// Spec world: a readable snapshot that sits BELOW the compaction floor
        /// (`SnapStale` — `snapCovered < firstIndex`). Trusting it would drop the
        /// compacted window and rewind the key generator, so recovery rejects
        /// loud: `RecoverOutcome = Reject` (FailClosed, #1065).
        #[test]
        fn stale_snapshot_below_floor_rejects() {
            use super::super::PersistedSnapshot;
            let (dir, _key1, _key2) = compacted_dir_with_two_instances("conf-stale");
            // Rewrite the surviving snapshot so it still deserializes but reports a
            // `covered_events` below the compaction floor (0 < first_index),
            // preserving the envelope header so the stale-floor guard — not a
            // decode failure — is what rejects it.
            let path = snapshot_file(&dir);
            let raw = fs::read(&path).unwrap();
            let nl = raw
                .iter()
                .position(|&b| b == b'\n')
                .expect("an enveloped snapshot carries a header line");
            let mut snap: PersistedSnapshot = serde_json::from_slice(&raw[nl + 1..]).unwrap();
            assert!(
                snap.covered_events > 0,
                "the floor must be nonzero to exercise the gap"
            );
            snap.covered_events = 0;
            let mut out = raw[..=nl].to_vec();
            out.extend_from_slice(&serde_json::to_vec(&snap).unwrap());
            fs::write(&path, out).unwrap();
            assert_recovery(&dir, &[], Expect::Rejects);
            let _ = fs::remove_dir_all(&dir);
        }

        /// Spec world: the snapshot + cold prefix are intact, but the SURVIVING
        /// ACTIVE (hot-tail) segment carries a complete, well-formed frame naming
        /// an event this build cannot decode (`DamageHotFrame` → `HotReadable` is
        /// false). A complete unknown frame is never a torn write, so the hot tail
        /// cannot be replayed and recovery must fail closed rather than silently
        /// dropping it: `RecoverOutcome = Reject` (FailClosed, #1066/#1070/#1065).
        /// This is the hot-segment analogue of `unreadable_cold_frame_rejects`.
        #[test]
        fn damaged_hot_segment_rejects() {
            use super::super::ACTIVE_NAME;
            let (dir, _key1, _key2) = compacted_dir_with_two_instances("conf-hotdamage");
            // Overwrite the surviving active segment (the hot tail past the
            // snapshot) with a complete unknown-variant frame.
            fs::write(dir.join(ACTIVE_NAME), "{\"NosuchHotEvent\":{}}\n").unwrap();
            assert_recovery(&dir, &[], Expect::Rejects);
            let _ = fs::remove_dir_all(&dir);
        }

        /// Builds a compacted TWO-partition world for the `recover_multi` path:
        /// two partitions share one segmented WAL, each owns one instance, a
        /// combined snapshot subsumes the sealed prefix, and that prefix is
        /// compacted off disk. Returns the dir + each partition's instance key.
        /// The multi-partition mirror of `compacted_dir_with_two_instances`.
        fn compacted_multi_dir(tag: &str) -> (std::path::PathBuf, u64, u64) {
            use std::sync::{Arc, atomic::AtomicU64};

            use nanobpmn_engine_core::{Engine, partition_of};

            use super::super::{compact_multi, write_multi_snapshot};
            use super::{demo, temp_dir};
            use crate::journal::{ExportBatch, Journal, SharedWriter};

            let dir = temp_dir(tag);
            // Keep the exporter receiver alive so shared writes have a wired cell.
            let (tx, _rx) = std::sync::mpsc::channel::<ExportBatch>();
            let (key0, key1) = {
                let (writer, recovery) =
                    SharedWriter::open_segmented(&dir, &[0, 1], 2, None).expect("open multi");
                let seg = Arc::clone(&recovery.shared);
                let mut engines: std::collections::HashMap<u64, Engine> =
                    recovery.engines.into_iter().collect();
                let mut j0 =
                    Journal::from_engine_shared(0, engines.remove(&0).unwrap(), true, &writer);
                let mut j1 =
                    Journal::from_engine_shared(1, engines.remove(&1).unwrap(), true, &writer);
                j0.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));
                j1.set_exporter(tx.clone(), Arc::new(AtomicU64::new(0)));

                let (deploy_events, _) = j0.apply_command(Command::DeployProcess(demo())).unwrap();
                j1.install_deployment(&deploy_events);
                let (e0, _) = j0.apply_command(Command::create_instance("demo")).unwrap();
                let key0 = e0.iter().find_map(|e| e.instance_key()).unwrap();
                let (e1, _) = j1.apply_command(Command::create_instance("demo")).unwrap();
                let key1 = e1.iter().find_map(|e| e.instance_key()).unwrap();
                assert_eq!(partition_of(key1), 1);

                let (snap0, covered0) = j0.snapshot_and_rotate().expect("snapshot p0");
                let (snap1, covered1) = j1.snapshot_and_rotate().expect("snapshot p1");
                write_multi_snapshot(&dir, vec![(0, covered0, snap0), (1, covered1, snap1)])
                    .expect("write combined snapshot");
                assert_eq!(
                    compact_multi(&seg, &[covered0, covered1], &[u64::MAX; 2]),
                    1,
                    "the sealed prefix is compacted for both partitions"
                );
                (key0, key1)
            };
            (dir, key0, key1)
        }

        /// Multi-partition NoSilentRewind: two partitions share one segmented WAL,
        /// the combined snapshot subsumes the compacted sealed prefix, and
        /// `recover_multi` reconstructs EVERY owned partition's full state
        /// (instance rows + the deployed definition) — never a strictly-older
        /// per-partition history. This anchors the spec's NoSilentRewind invariant
        /// to the `recover_multi` migration path, which the single-partition
        /// scenarios above do not exercise.
        #[test]
        fn multi_partition_reconstructs_full() {
            use nanobpmn_engine_core::Engine;

            use super::super::recover_multi;

            let (dir, key0, key1) = compacted_multi_dir("conf-multi-full");
            let recovery = recover_multi(&dir, &[0, 1], 2, None).expect("recover multi");
            assert!(
                !recovery.fresh,
                "recovered multi state must not read as fresh"
            );
            let engines: std::collections::HashMap<u64, Engine> =
                recovery.engines.into_iter().collect();
            assert!(
                engines[&0].instance(key0).is_some(),
                "NoSilentRewind violated: partition 0 lost its instance {key0}"
            );
            assert!(
                engines[&1].instance(key1).is_some(),
                "NoSilentRewind violated: partition 1 lost its instance {key1}"
            );
            assert!(
                !engines[&0].state().processes.is_empty(),
                "NoSilentRewind violated: partition 0 lost the deployed definition"
            );
            let _ = fs::remove_dir_all(&dir);
        }

        /// Multi-partition FailClosed: the combined snapshot loads but is stale for
        /// one owned partition (`covered < pp_base[p]`), so trusting it would
        /// replay only that shard's surviving tail across the compacted gap and
        /// rewind its key generator (#1065). `recover_multi` must reject loud:
        /// `RecoverOutcome = Reject` (FailClosed). Anchors FailClosed to the
        /// multi-partition path.
        #[test]
        fn multi_partition_stale_partition_snapshot_rejects() {
            use super::super::{MULTI_SNAP_NAME, MultiPersistedSnapshot, recover_multi};

            let (dir, _key0, _key1) = compacted_multi_dir("conf-multi-stale");
            // Rewrite partition 1's entry so its `covered` sits below the
            // compaction floor while the enveloped payload still parses — the
            // per-partition stale-floor guard (not a decode failure) must reject.
            let path = dir.join(MULTI_SNAP_NAME);
            let raw = fs::read(&path).unwrap();
            let nl = raw
                .iter()
                .position(|&b| b == b'\n')
                .expect("an enveloped multi-snapshot carries a header line");
            let mut snap: MultiPersistedSnapshot = serde_json::from_slice(&raw[nl + 1..]).unwrap();
            let e = snap
                .entries
                .iter_mut()
                .find(|e| e.partition == 1)
                .expect("partition 1 snapshot entry");
            assert!(
                e.covered > 0,
                "partition 1's compaction floor must be nonzero"
            );
            e.covered = 0;
            let mut out = raw[..=nl].to_vec();
            out.extend_from_slice(&serde_json::to_vec(&snap).unwrap());
            fs::write(&path, out).unwrap();

            let err = recover_multi(&dir, &[0, 1], 2, None)
                .err()
                .expect("recovery must refuse a stale partition snapshot, not rewind");
            assert_eq!(
                err.kind(),
                io::ErrorKind::InvalidData,
                "fail-closed multi recovery must surface a typed InvalidData reject: {err}"
            );
            let _ = fs::remove_dir_all(&dir);
        }
    }
}
