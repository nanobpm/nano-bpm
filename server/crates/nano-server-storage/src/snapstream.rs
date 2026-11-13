//! Streaming/sidecar snapshot capture and restore (#1337).
//!
//! # The defect this fixes
//!
//! A *self-contained* classic snapshot must carry every instance's variables —
//! including the instances that were spilled off-heap precisely because the live
//! working set exceeds available RAM. The pre-#1337 capture
//! ([`Journal::engine_snapshot`](crate::journal::Journal)) folded **every** cold
//! instance and **every** spilled variable map back into a single in-memory
//! [`nanobpmn_engine_core::EngineSnapshot`] before serializing it, so a periodic
//! or Raft snapshot transiently recreated the entire working set on the engine
//! heap at once — a long actor stall, or OOM, on exactly the memory-constrained
//! nodes spill targets.
//!
//! # The shape of the fix
//!
//! A snapshot is still self-contained and still restores every variable — but it
//! is **assembled as a stream, not materialized as one value**:
//!
//! - The **control** half (every [`State`](nanobpmn_engine_core::State) map
//!   except the per-instance payloads, plus the scalar generator/clock metadata)
//!   is small and is serialized directly.
//! - Each **instance** (its control state, jobs, timers, subscriptions and
//!   variables) becomes one self-contained
//!   [`InstanceSnapshot`](nanobpmn_engine_core::InstanceSnapshot). Resident
//!   instances are lifted out of the cloned state one at a time; **cold**
//!   instances are read row-by-row from the spill store and *never become
//!   resident*; a variable-spilled instance's payload is resolved from the spill
//!   tier as it is streamed. Each is appended to the current **chunk** and the
//!   chunk is flushed (and dropped) once it reaches a size bound — so the peak
//!   heap is bounded by one chunk, never by the whole cold set.
//!
//! Restore is the mirror: read the control half, then stream the instance chunks
//! back, restoring one instance at a time.
//!
//! # On-disk layout
//!
//! The sidecar is a single file of newline-delimited JSON **frames**:
//!
//! ```text
//! {"format_version":3,"incarnation":…,"engine_fingerprint":"…"}\n   (header)
//! {"covered_events":…,"control":{…}}\n                              (control frame)
//! {"instances":[<InstanceSnapshot>, …]}\n                           (chunk frame, ×N)
//! {"instances":[],"end":true}\n                                     (terminal frame)
//! ```
//!
//! Each frame is one compact JSON line, so a reader streams it line-by-line with
//! a `BufReader` and never holds more than one chunk in memory. The header is
//! the same always-parseable envelope [`crate::seglog`] stamps on a snapshot, so
//! a reader can learn the `format_version` before attempting the body.

use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::Path;
use std::sync::Arc;

use nanobpmn_engine_core::{EngineSnapshot, InstanceSnapshot, Key};

use crate::varspill::VarSpillStore;
use crate::varstore::VarStore;

/// The instance-chunk size bound: a chunk frame is flushed once it holds this
/// many instances. This bounds the number of instance payloads resident at once
/// during both capture and restore to a small constant, independent of how large
/// the cold/spilled working set is. (A *byte* bound would be tighter still, but
/// the instance count is what the capture loop naturally controls, and the
/// per-instance payload is what spill already bounds.)
pub const STREAM_CHUNK_INSTANCES: usize = 64;

/// One streamed instance chunk: a bounded batch of self-contained instance
/// payloads. `end` marks the terminal frame, so a reader can distinguish "clean
/// end of stream" from a truncated file (a torn write must fail closed, not
/// silently restore a prefix of the instances).
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct InstanceChunk {
    instances: Vec<InstanceSnapshot>,
    #[serde(default)]
    end: bool,
}

/// The control frame: everything in the snapshot **except** the per-instance
/// payloads, plus the absolute event count the snapshot covers. `control` is the
/// full [`EngineSnapshot`] with `state.instances` emptied (and the per-instance
/// jobs/timers/subscriptions/incidents lifted out into the instance chunks).
#[derive(serde::Serialize, serde::Deserialize)]
pub(crate) struct ControlFrame {
    covered_events: u64,
    control: EngineSnapshot,
    /// The rebase-floor identity this snapshot was stamped with (#1405), see
    /// `seglog::PersistedSnapshot::base_id`. Additive: absent decodes as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    base_id: Option<u64>,
}

/// The decomposed halves of a streaming capture, produced on the engine thread
/// (a consistent read of the live state) and consumed off it (the streaming
/// serialize). Splitting the two lets the Raft snapshot builder capture on the
/// state-machine worker and stream to disk from a blocking task, so the engine
/// actor never performs the (potentially long) serialize.
///
/// `resident` holds the resident instances' self-contained payloads; `control`
/// is the same engine state with `instances` emptied. Neither contains the cold
/// set — cold instances are read from the spill store as they stream out.
pub struct StreamingCapture {
    pub control: EngineSnapshot,
    pub resident: Vec<InstanceSnapshot>,
}

impl StreamingCapture {
    /// Reassembles a full, self-contained [`EngineSnapshot`] from the control
    /// half plus every instance payload (resident and cold alike). Used on the
    /// *restore* side, where the whole working set is about to become resident
    /// anyway (a snapshot install / boot rebuilds the engine in memory).
    pub fn reassemble(mut self, cold: Vec<InstanceSnapshot>) -> EngineSnapshot {
        for snap in self.resident.into_iter().chain(cold) {
            nanobpmn_engine_core::restore_instance_snapshot(&mut self.control.state, snap);
        }
        self.control
    }
}

/// The parameters of a streaming snapshot write, bundled to keep
/// [`write_streaming_snapshot`]'s arity readable. See that function's docs for
/// the semantics of each field.
pub struct StreamWrite<'a, I, RC, RS>
where
    I: IntoIterator<Item = InstanceSnapshot>,
    RC: FnMut(Key) -> std::io::Result<InstanceSnapshot>,
    RS: FnMut(&mut InstanceSnapshot) -> std::io::Result<()>,
{
    pub path: &'a Path,
    pub header_line: &'a str,
    pub covered_events: u64,
    /// The rebase-floor identity to stamp into the control frame (#1405).
    pub base_id: Option<u64>,
    pub control: EngineSnapshot,
    pub instances: I,
    pub cold: &'a [Key],
    pub read_cold: RC,
    pub resolve_spilled: RS,
    /// When true, a variable-spilled instance is serialized **as-is** (empty
    /// placeholder + the spilled flag), never resolved through `resolve_spilled`.
    /// This is the lean-snapshot mode (#1463 review): the authoritative durable
    /// var store holds the payload in lockstep, so the snapshot is control-only
    /// by design and resolving the placeholder would both fail (no spill store
    /// is wired for it) and be wrong (the snapshot must stay variable-free).
    /// When false (the self-contained default), every spilled flag is resolved
    /// so the snapshot restores every variable with no dependency on any store.
    pub control_only: bool,
}

/// Streams a self-contained snapshot to `path` without ever holding the whole
/// cold set in memory.
///
/// * `header_line` is the always-parseable envelope header (already serialized,
///   no trailing newline) — the caller stamps the format version into it.
/// * `covered_events` is the absolute journal-event count the snapshot subsumes.
/// * `control` is the snapshot skeleton (instances already emptied).
/// * `instances` yields the decomposed per-instance payloads **for the resident
///   set** (consumed one at a time, dropped after their chunk flushes).
/// * `cold` is the list of wholly off-heap instance keys, each resolved lazily
///   from the spill store via `read_cold` as it is streamed, so a cold instance
///   never becomes resident.
/// * `resolve_spilled` fills a variable-spilled instance's payload in place from
///   the spill tier just before it is serialized.
///
/// Fail-closed: any I/O error, any cold row that will not read, or any spilled
/// payload that cannot be resolved aborts the write (returns `Err`) and removes
/// the partial file, so an incomplete snapshot is never sealed — the caller must
/// treat `Err` exactly like the existing fail-closed capture returning `None`.
///
/// The two closures are injected so this module stays free of the journal's
/// spill-store wiring (and unit-testable without one).
///
/// The parameters are bundled into [`StreamWrite`] to keep the arity readable.
pub fn write_streaming_snapshot<I, RC, RS>(req: StreamWrite<'_, I, RC, RS>) -> std::io::Result<()>
where
    I: IntoIterator<Item = InstanceSnapshot>,
    RC: FnMut(Key) -> std::io::Result<InstanceSnapshot>,
    RS: FnMut(&mut InstanceSnapshot) -> std::io::Result<()>,
{
    let StreamWrite {
        path,
        header_line,
        covered_events,
        base_id,
        control,
        instances,
        cold,
        mut read_cold,
        mut resolve_spilled,
        control_only,
    } = req;
    let tmp = path.with_extension("tmp");
    let outcome = write_streaming_snapshot_inner(
        &tmp,
        header_line,
        covered_events,
        base_id,
        control,
        instances,
        cold,
        &mut read_cold,
        &mut resolve_spilled,
        control_only,
    );
    match outcome {
        Ok(()) => {
            // fsync the file content before the rename so a crash cannot leave a
            // renamed-but-torn snapshot; then atomically promote.
            let f = File::open(&tmp)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp, path)?;
            // fsync the containing directory so the rename itself is durable
            // across a crash/power loss (the file content fsync alone does not
            // guarantee the directory entry is persisted).
            if let Some(dir) = path.parent()
                && let Ok(d) = File::open(dir)
            {
                let _ = d.sync_all();
            }
            Ok(())
        }
        Err(e) => {
            // Never leave a partial snapshot behind to be mistaken for a complete
            // one on recovery.
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

#[allow(clippy::too_many_arguments)]
fn write_streaming_snapshot_inner<I, RC, RS>(
    tmp: &Path,
    header_line: &str,
    covered_events: u64,
    base_id: Option<u64>,
    control: EngineSnapshot,
    instances: I,
    cold: &[Key],
    read_cold: &mut RC,
    resolve_spilled: &mut RS,
    control_only: bool,
) -> std::io::Result<()>
where
    I: IntoIterator<Item = InstanceSnapshot>,
    RC: FnMut(Key) -> std::io::Result<InstanceSnapshot>,
    RS: FnMut(&mut InstanceSnapshot) -> std::io::Result<()>,
{
    let file = File::create(tmp)?;
    let mut w = BufWriter::new(file);

    // Header (always-parseable envelope line) then the control frame.
    w.write_all(header_line.as_bytes())?;
    w.write_all(b"\n")?;
    serde_json::to_writer(
        &mut w,
        &ControlFrame {
            covered_events,
            control,
            base_id,
        },
    )
    .map_err(into_invalid_data)?;
    w.write_all(b"\n")?;

    // Flush one chunk frame: serialize + newline, then drop the batch. At most
    // STREAM_CHUNK_INSTANCES payloads are live in the writer at once.
    fn flush_chunk(
        w: &mut BufWriter<File>,
        chunk: &mut Vec<InstanceSnapshot>,
        end: bool,
    ) -> std::io::Result<()> {
        if chunk.is_empty() && !end {
            return Ok(());
        }
        let frame = InstanceChunk {
            instances: std::mem::take(chunk),
            end,
        };
        serde_json::to_writer(&mut *w, &frame).map_err(into_invalid_data)?;
        w.write_all(b"\n")?;
        Ok(())
    }

    let mut chunk: Vec<InstanceSnapshot> = Vec::with_capacity(STREAM_CHUNK_INSTANCES);

    // Resident instances: resolve a spilled payload just before it serializes,
    // then stream it out — UNLESS control_only (lean mode), which serializes the
    // placeholder as-is. Consuming `instances` by value drops each payload as
    // its chunk flushes.
    for mut snap in instances {
        if snap.instance.variables_spilled && !control_only {
            resolve_spilled(&mut snap)?;
        }
        chunk.push(snap);
        if chunk.len() >= STREAM_CHUNK_INSTANCES {
            flush_chunk(&mut w, &mut chunk, false)?;
        }
    }

    // Cold instances: read one row straight from the spill store, push it, flush
    // when the chunk fills — the cold set never becomes resident. A cold row can
    // itself be variable-spilled (spilled before it was cold-evicted), so it is
    // resolved through the same `resolve_spilled` path as a resident instance to
    // keep the snapshot self-contained (skipped under control_only).
    for &key in cold {
        let mut snap = read_cold(key)?;
        if snap.instance.variables_spilled && !control_only {
            resolve_spilled(&mut snap)?;
        }
        chunk.push(snap);
        if chunk.len() >= STREAM_CHUNK_INSTANCES {
            flush_chunk(&mut w, &mut chunk, false)?;
        }
    }

    // Terminal frame: flushes any remainder and marks a clean end-of-stream.
    flush_chunk(&mut w, &mut chunk, true)?;
    w.flush()?;
    Ok(())
}

/// The off-heap stores a streaming write resolves against (#1337). Bundled to
/// keep [`write_streaming_snapshot_from_stores`]'s arity readable. Each tier is
/// optional (a journal may run without spill, cold, or a lean var store); a
/// missing tier that a captured row needs fails the write closed.
#[derive(Clone, Default)]
pub struct SpillStores {
    /// The cold tier's store: whole-instance `InstanceSnapshot` rows.
    pub cold: Option<Arc<VarSpillStore>>,
    /// The classic variable-spill cache.
    pub spill: Option<Arc<VarSpillStore>>,
    /// The lean mode's authoritative variable store.
    pub varstore: Option<Arc<VarStore>>,
    /// Legacy fallback tier consulted before a lean var-store miss is declared a
    /// definitive absence (#1574 review): the cold store holds whole-instance
    /// rows (with their variables inline) for instances that were cold-shed, so
    /// a var-store miss for such an instance is a routing artifact, not a loss.
    /// [`Journal::peek_spilled_variables`](crate::journal::Journal) makes the
    /// same check (spill-or-cold) before refusing to declare a loss; without it
    /// here a cold-only configuration quarantines recoverable variables.
    pub legacy: Option<Arc<VarSpillStore>>,
    /// Where a DEFINITIVELY absent row (readable store, no row) is recorded
    /// before the write fails closed (#1447), so the off-thread Raft snapshot
    /// writer still reports the orphan to the leader-propose machinery.
    pub absences: Option<crate::journal::OrphanInbox>,
}

/// Store-backed streaming write (#1337): the production entry point. Streams
/// `capture` (control skeleton + resident payloads) plus the cold set to `path`,
/// resolving off-heap rows against the wired stores:
///
/// - a **cold** instance's whole `InstanceSnapshot` is read row-by-row from
///   `stores.cold` (never becoming resident);
/// - a **variable-spilled** resident instance's payload is read from
///   `stores.varstore` (lean mode) or `stores.spill` (classic), and its spilled
///   flag cleared in the serialized copy so the snapshot is self-contained.
///
/// `control_only` (the lean checkpoint, #1463 review) instead serializes a
/// variable-spilled placeholder **as-is**: the authoritative var store holds the
/// payload in lockstep, so the snapshot stays variable-free by design and the
/// placeholder is never resolved.
///
/// Purely synchronous and owning only `Arc` store handles — safe to call from a
/// blocking task off the engine actor. Fail-closed: any unreadable/absent row
/// aborts the write and removes the partial file.
#[allow(clippy::too_many_arguments)]
pub fn write_streaming_snapshot_from_stores(
    path: &Path,
    header_line: &str,
    covered_events: u64,
    base_id: Option<u64>,
    capture: StreamingCapture,
    cold_keys: &[Key],
    stores: SpillStores,
    control_only: bool,
) -> std::io::Result<()> {
    let SpillStores {
        cold: cold_store,
        spill: spill_store,
        varstore,
        legacy,
        absences,
    } = stores;
    write_streaming_snapshot(StreamWrite {
        path,
        header_line,
        covered_events,
        base_id,
        control: capture.control,
        instances: capture.resident,
        cold: cold_keys,
        read_cold: {
            let absences = absences.clone();
            move |key| read_cold_from_store(key, cold_store.as_ref(), absences.as_ref())
        },
        resolve_spilled: move |snap| {
            resolve_spilled_from_stores(
                snap,
                varstore.as_ref(),
                spill_store.as_ref(),
                legacy.as_ref(),
                absences.as_ref(),
            )
        },
        control_only,
    })
}

/// Reads a streaming snapshot back, invoking `restore_control` with the control
/// skeleton (and covered-event count) first and then `restore` with each
/// instance one at a time, so the restore peak is bounded by one chunk rather
/// than the whole instance set.
///
/// Fail-closed on a torn write: if the stream ends without the terminal `end`
/// frame, this returns `Err` (the snapshot is incomplete and must not be used as
/// a recovery point).
pub fn read_streaming_snapshot<R>(
    path: &Path,
    restore_control: impl FnMut(EngineSnapshot, u64),
    restore: R,
) -> std::io::Result<()>
where
    R: FnMut(InstanceSnapshot),
{
    let file = File::open(path)?;
    read_streaming_snapshot_from(file, restore_control, restore)
}

/// The reader-from counterpart of [`read_streaming_snapshot`]: streams from any
/// positioned [`std::io::Read`] (e.g. an already-open, rewound snapshot file), so
/// a caller that holds the file handle (the Raft install path) does not reopen
/// it. The reader is wrapped in a `BufReader` internally.
pub fn read_streaming_snapshot_from<RD, R>(
    reader: RD,
    restore_control: impl FnMut(EngineSnapshot, u64),
    restore: R,
) -> std::io::Result<()>
where
    RD: std::io::Read,
    R: FnMut(InstanceSnapshot),
{
    read_streaming_snapshot_based_from(reader, restore_control, restore).map(|_| ())
}

/// [`read_streaming_snapshot`] that also returns the control frame's rebase-floor
/// identity (#1405; `None` when absent, incl. the legacy headerless layout).
pub fn read_streaming_snapshot_based<R>(
    path: &Path,
    restore_control: impl FnMut(EngineSnapshot, u64),
    restore: R,
) -> std::io::Result<Option<u64>>
where
    R: FnMut(InstanceSnapshot),
{
    let file = File::open(path)?;
    read_streaming_snapshot_based_from(file, restore_control, restore)
}

fn read_streaming_snapshot_based_from<RD, R>(
    reader: RD,
    mut restore_control: impl FnMut(EngineSnapshot, u64),
    mut restore: R,
) -> std::io::Result<Option<u64>>
where
    RD: std::io::Read,
    R: FnMut(InstanceSnapshot),
{
    let mut reader = BufReader::new(reader);

    // Line 1: the envelope header (its format version is validated by the caller
    // before this is invoked). The line is consumed to advance past it; the body
    // is not inspected here.
    let mut header_line = String::new();
    if reader.read_line(&mut header_line)? == 0 {
        return Err(invalid_data("streaming snapshot is empty (no header)"));
    }

    // Legacy fallback (#1464 review): a snapshot written by a build predating the
    // chunked sidecar framing is ONE headerless `EngineSnapshot` JSON value (the
    // pre-#1337 layout), with no envelope header line. Mistaking its first line
    // for a header would consume it and then fail with "no control frame",
    // breaking boot recovery / install of a durable pre-upgrade snapshot (the
    // #1065 replay-compat class). Detect the legacy shape — line 1 is NOT an
    // envelope header — and decode the WHOLE stream as a single `EngineSnapshot`,
    // then replay it through the same control/per-instance restore contract so
    // the caller cannot tell the difference. The envelope marker `format_version`
    // is fixed-for-all-time (see `seglog::SnapshotHeader`), so its presence on
    // line 1 unambiguously marks the chunked framing; its absence marks legacy.
    if !is_envelope_header_line(&header_line) {
        return restore_legacy_headerless_snapshot(header_line, reader, restore_control, restore)
            .map(|()| None);
    }

    // Line 2: the control frame.
    let mut control_line = String::new();
    if reader.read_line(&mut control_line)? == 0 {
        return Err(invalid_data("streaming snapshot has no control frame"));
    }
    let control_frame: ControlFrame =
        serde_json::from_str(&control_line).map_err(into_invalid_data)?;
    let base_id = control_frame.base_id;
    restore_control(control_frame.control, control_frame.covered_events);

    // Remaining lines: instance chunk frames until the terminal `end` frame.
    let mut saw_end = false;
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        let chunk: InstanceChunk = serde_json::from_str(trimmed).map_err(into_invalid_data)?;
        let end = chunk.end;
        for snap in chunk.instances {
            restore(snap);
        }
        if end {
            saw_end = true;
            break;
        }
    }
    if !saw_end {
        return Err(invalid_data(
            "streaming snapshot is truncated (no terminal frame); refusing to \
             restore an incomplete instance set",
        ));
    }
    Ok(base_id)
}

fn into_invalid_data(e: serde_json::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}

fn invalid_data(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string())
}

/// Whether a snapshot's first line is the always-parseable envelope header that
/// prefixes the chunked sidecar framing. The header shape is fixed for all time
/// (`seglog::SnapshotHeader`: `{"format_version":N,"incarnation":…,…}`), so the
/// `format_version` key is the unambiguous marker. A legacy headerless snapshot
/// is a bare `EngineSnapshot` (`{"state":…,"partition_id":…,…}`) whose first line
/// never carries `format_version`, so this cleanly separates the two layouts.
///
/// The check parses the line as a generic JSON object and looks for the key,
/// rather than a brittle substring match, so a legacy payload that happened to
/// contain the substring inside a string value is not misclassified.
fn is_envelope_header_line(line: &str) -> bool {
    let Ok(serde_json::Value::Object(map)) = serde_json::from_str::<serde_json::Value>(line.trim())
    else {
        return false;
    };
    map.contains_key("format_version")
}

/// Restores a **legacy headerless** snapshot: a single bare `EngineSnapshot`
/// JSON value (the pre-#1337 layout), with no envelope header and no chunk
/// framing. `first_line` is the already-consumed first line of the stream;
/// `reader` yields the remainder. The whole body is decoded as one
/// `EngineSnapshot` (exactly as the pre-#1337 reader did with
/// `serde_json::from_reader`), then decomposed and replayed through the
/// streaming contract: `restore_control` once with the control skeleton, then
/// `restore` once per instance.
///
/// A legacy snapshot pre-dates self-contained spill metadata, so it carries no
/// `covered_events`; `0` is reported (the Raft install/boot callers take the log
/// position from the snapshot `meta.last_log_id`, not the in-file count, and the
/// reassembly reader ignores it). Memory note: a legacy file was always read
/// whole, so buffering it here matches the legacy read's footprint — the bounded
/// streaming path applies only to the new chunked framing.
fn restore_legacy_headerless_snapshot<R, RD>(
    first_line: String,
    mut reader: BufReader<RD>,
    mut restore_control: impl FnMut(EngineSnapshot, u64),
    mut restore: R,
) -> std::io::Result<()>
where
    R: FnMut(InstanceSnapshot),
    RD: std::io::Read,
{
    // Reassemble the full body: the consumed first line plus the rest of the
    // stream. A legacy snapshot is a single compact JSON value, so it is one
    // logical document even if it spans multiple lines.
    let mut body = first_line;
    reader.read_to_string(&mut body)?;
    let snapshot: EngineSnapshot = serde_json::from_str(&body).map_err(into_invalid_data)?;

    // Split the whole snapshot into the control skeleton + one self-contained
    // payload per instance, then replay them through the streaming contract so a
    // caller (e.g. `read_streaming_engine_snapshot_from`) restores a legacy file
    // exactly as it does a chunked one.
    let EngineSnapshot {
        state,
        partition_id,
        next_local,
        num_partitions,
        now,
        start_dispatch_rr,
        raft_applied,
    } = snapshot;
    let (control_state, instances) =
        nanobpmn_engine_core::Engine::decompose_state_for_streaming(state);
    let control = EngineSnapshot {
        state: control_state,
        partition_id,
        next_local,
        num_partitions,
        now,
        start_dispatch_rr,
        raft_applied,
    };
    restore_control(control, 0);
    for snap in instances {
        restore(snap);
    }
    Ok(())
}

/// Reads a streaming snapshot from a positioned reader and reassembles the full,
/// self-contained [`EngineSnapshot`] (control skeleton + every instance payload
/// restored into it). Used where the whole working set is about to become
/// resident anyway — a Raft snapshot install or a boot restore — so a single
/// materialized snapshot is the goal, but the *read* still streams chunk-by-chunk
/// rather than holding the whole serialized body as one `Vec<u8>`.
pub fn read_streaming_engine_snapshot_from<RD: std::io::Read>(
    reader: RD,
) -> std::io::Result<EngineSnapshot> {
    let acc: std::cell::RefCell<Option<EngineSnapshot>> = std::cell::RefCell::new(None);
    read_streaming_snapshot_from(
        reader,
        |control, _covered| *acc.borrow_mut() = Some(control),
        |snap| {
            let mut guard = acc.borrow_mut();
            if let Some(acc) = guard.as_mut() {
                nanobpmn_engine_core::restore_instance_snapshot(&mut acc.state, snap);
            }
        },
    )?;
    acc.into_inner()
        .ok_or_else(|| invalid_data("streaming snapshot had no control frame"))
}

/// The path-based counterpart of [`read_streaming_engine_snapshot_from`].
pub fn read_streaming_engine_snapshot(path: &Path) -> std::io::Result<EngineSnapshot> {
    let file = File::open(path)?;
    read_streaming_engine_snapshot_from(file)
}

// ----------------------------------------------------------------------------
// Multi-partition combined streaming snapshot (#1463).
//
// The multi-partition `msnapshot.bin` carries one engine per OWNED partition.
// Its v3 framing reuses the same bounded-chunk idea, generalized so each
// instance chunk is tagged with the partition it belongs to:
//
// ```text
// {"format_version":3,…}\n                                            (header)
// {"partition":0,"covered_events":…,"control":{…}}\n        (control frame ×P)
// {"partition":0,"instances":[<InstanceSnapshot>, …]}\n    (chunk frame ×N)
// {"partition":-,"instances":[],"end":true}\n                     (terminal)
// ```
//
// Control frames carry no instances (each partition's skeleton); every instance
// payload — resident or cold — streams through the shared bounded chunk frames,
// so no partition's whole cold set is ever resident at once.
// ----------------------------------------------------------------------------

/// One partition's control frame in a combined multi-partition snapshot.
#[derive(serde::Serialize, serde::Deserialize)]
struct MultiControlFrame {
    partition: u64,
    covered_events: u64,
    control: EngineSnapshot,
    /// The partition's rebase-floor identity (#1405), see [`ControlFrame`].
    /// Additive: absent decodes as `None`.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    base_id: Option<u64>,
}

/// One instance chunk in a combined multi-partition snapshot, tagged with the
/// partition its instances belong to so the reader restores each into the right
/// partition's engine. `end` marks the terminal frame (fail-closed on a torn
/// write, exactly like the single-partition stream).
#[derive(serde::Serialize, serde::Deserialize)]
struct MultiInstanceChunk {
    partition: u64,
    instances: Vec<InstanceSnapshot>,
    #[serde(default)]
    end: bool,
}

/// Streams a combined multi-partition snapshot to `path` without ever holding
/// any partition's whole cold set in memory (#1463). `entries` is
/// `(partition, covered_events, capture, cold_keys, stores, control_only)` per
/// owned partition; each partition's control frame is written first, then every
/// partition's instances stream through shared bounded chunk frames (cold rows
/// read lazily from that partition's stores). `control_only` (the lean
/// checkpoint) serializes a variable-spilled placeholder as-is rather than
/// resolving it. Fail-closed and atomic (tmp + fsync + rename, partial file
/// removed on error), exactly like [`write_streaming_snapshot_from_stores`].
///
/// `base_ids` maps a partition to the rebase-floor identity stamped into its
/// control frame (#1405); a partition absent from the map is written without one.
pub fn write_multi_streaming_snapshot(
    path: &Path,
    header_line: &str,
    entries: Vec<(u64, u64, StreamingCapture, Vec<Key>, SpillStores, bool)>,
    base_ids: &std::collections::HashMap<u64, u64>,
) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    let outcome = write_multi_streaming_snapshot_inner(&tmp, header_line, entries, base_ids);
    match outcome {
        Ok(()) => {
            let f = File::open(&tmp)?;
            f.sync_all()?;
            drop(f);
            std::fs::rename(&tmp, path)?;
            if let Some(dir) = path.parent()
                && let Ok(d) = File::open(dir)
            {
                let _ = d.sync_all();
            }
            Ok(())
        }
        Err(e) => {
            let _ = std::fs::remove_file(&tmp);
            Err(e)
        }
    }
}

fn write_multi_streaming_snapshot_inner(
    tmp: &Path,
    header_line: &str,
    entries: Vec<(u64, u64, StreamingCapture, Vec<Key>, SpillStores, bool)>,
    base_ids: &std::collections::HashMap<u64, u64>,
) -> std::io::Result<()> {
    let file = File::create(tmp)?;
    let mut w = BufWriter::new(file);

    w.write_all(header_line.as_bytes())?;
    w.write_all(b"\n")?;

    // Every partition's control frame first (instance-free skeletons).
    for (partition, covered_events, capture, _, _, _) in &entries {
        serde_json::to_writer(
            &mut w,
            &MultiControlFrame {
                partition: *partition,
                covered_events: *covered_events,
                control: capture.control.clone(),
                base_id: base_ids.get(partition).copied(),
            },
        )
        .map_err(into_invalid_data)?;
        w.write_all(b"\n")?;
    }

    // Then stream every partition's instances through shared bounded chunks.
    let mut chunk: Vec<(u64, InstanceSnapshot)> = Vec::with_capacity(STREAM_CHUNK_INSTANCES);
    for (partition, _covered, capture, cold_keys, stores, control_only) in entries {
        let SpillStores {
            cold: cold_store,
            spill: spill_store,
            varstore,
            legacy,
            absences,
        } = stores;
        // Reuse the single-partition spill-resolution closure logic per row.
        let resolve = |snap: &mut InstanceSnapshot| {
            resolve_spilled_from_stores(
                snap,
                varstore.as_ref(),
                spill_store.as_ref(),
                legacy.as_ref(),
                absences.as_ref(),
            )
        };
        let push = |chunk: &mut Vec<(u64, InstanceSnapshot)>,
                    w: &mut BufWriter<File>,
                    mut snap: InstanceSnapshot|
         -> std::io::Result<()> {
            // Lean (control_only): serialize the spilled placeholder as-is — the
            // authoritative var store holds the payload. Self-contained: resolve.
            if snap.instance.variables_spilled && !control_only {
                resolve(&mut snap)?;
            }
            chunk.push((partition, snap));
            if chunk.len() >= STREAM_CHUNK_INSTANCES {
                flush_multi_chunk(w, chunk, false)?;
            }
            Ok(())
        };
        for snap in capture.resident {
            push(&mut chunk, &mut w, snap)?;
        }
        for key in cold_keys {
            let snap = read_cold_from_store(key, cold_store.as_ref(), absences.as_ref())?;
            push(&mut chunk, &mut w, snap)?;
        }
    }
    flush_multi_chunk(&mut w, &mut chunk, true)?;
    w.flush()?;
    Ok(())
}

fn flush_multi_chunk(
    w: &mut BufWriter<File>,
    chunk: &mut Vec<(u64, InstanceSnapshot)>,
    end: bool,
) -> std::io::Result<()> {
    if chunk.is_empty() && !end {
        return Ok(());
    }
    // A chunk may span partitions; emit one frame per contiguous partition run
    // so each frame is single-partition (simpler reader). For the common case
    // (one partition's rows per chunk) this is exactly one frame. Drain takes
    // ownership safely — no unsafe, no leak.
    let taken = std::mem::take(chunk);
    let mut runs: Vec<(u64, Vec<InstanceSnapshot>)> = Vec::new();
    for (partition, snap) in taken {
        match runs.last_mut() {
            Some((p, batch)) if *p == partition => batch.push(snap),
            _ => runs.push((partition, vec![snap])),
        }
    }
    for (partition, batch) in runs {
        let frame = MultiInstanceChunk {
            partition,
            instances: batch,
            end: false,
        };
        serde_json::to_writer(&mut *w, &frame).map_err(into_invalid_data)?;
        w.write_all(b"\n")?;
    }
    if end {
        let frame = MultiInstanceChunk {
            partition: 0,
            instances: Vec::new(),
            end: true,
        };
        serde_json::to_writer(&mut *w, &frame).map_err(into_invalid_data)?;
        w.write_all(b"\n")?;
    }
    Ok(())
}

/// Resolves a variable-spilled instance's payload from the wired stores,
/// mirroring [`write_streaming_snapshot_from_stores`]'s fail-closed semantics
/// (a lean var-store miss with a surviving legacy spill row is refused).
/// Reads a cold instance's row, failing closed when absent or unreadable. A
/// DEFINITIVE absence is recorded in `absences` first (#1447).
fn read_cold_from_store(
    key: Key,
    cold_store: Option<&Arc<VarSpillStore>>,
    absences: Option<&crate::journal::OrphanInbox>,
) -> std::io::Result<InstanceSnapshot> {
    let store = cold_store
        .ok_or_else(|| invalid_data("cold index references a key but no cold store is wired"))?;
    match store
        .try_get_cold(key)
        .map_err(|e| invalid_data(&e.to_string()))?
    {
        Some(snap) => Ok(snap),
        None => {
            if let Some(absences) = absences {
                absences.record(key, nanobpmn_engine_core::OrphanKind::Cold);
            }
            Err(invalid_data(&format!(
                "cold instance {key} has no row in the spill store"
            )))
        }
    }
}

fn resolve_spilled_from_stores(
    snap: &mut InstanceSnapshot,
    varstore: Option<&Arc<VarStore>>,
    spill_store: Option<&Arc<VarSpillStore>>,
    legacy: Option<&Arc<VarSpillStore>>,
    absences: Option<&crate::journal::OrphanInbox>,
) -> std::io::Result<()> {
    let key = snap.instance.key;
    let resolved = if let Some(vs) = varstore {
        match vs.try_get(key).map_err(|e| invalid_data(&e.to_string()))? {
            Some(vars) => Some(vars),
            None => {
                // A lean var-store miss is a definitive absence ONLY once the
                // legacy tier (spill-or-cold, mirroring
                // `Journal::peek_spilled_variables`) is wired and also has no
                // row (#1574 review): a cold-shed instance's payload lives in
                // its cold row, so a cold-only configuration must not report
                // the miss as a loss.
                let legacy = spill_store.or(legacy);
                let Some(legacy_store) = legacy else {
                    // No legacy tier to check: the miss proves nothing about a
                    // possibly unmigrated legacy row — fail closed, do NOT
                    // record an absence.
                    return Err(invalid_data(&format!(
                        "spilled instance {key}: var store has no row and no legacy \
                         spill/cold tier is wired to rule out an unmigrated legacy row; \
                         refusing to declare the payload lost"
                    )));
                };
                // The legacy tier holds a payload in EITHER its variable-spill
                // row or its whole-instance cold row (which carries variables
                // inline); either means the payload survives.
                let legacy_row = legacy_store
                    .try_has_legacy_payload(key)
                    .map_err(|e| invalid_data(&e.to_string()))?;
                if legacy_row {
                    return Err(invalid_data(&format!(
                        "spilled instance {key}: var store has no row but a legacy \
                         spill/cold row exists (unmigrated lean-over-legacy state); \
                         refusing to seal a potentially stale payload"
                    )));
                }
                None
            }
        }
    } else if let Some(sp) = spill_store {
        sp.try_get(key).map_err(|e| invalid_data(&e.to_string()))?
    } else {
        None
    };
    match resolved {
        Some(vars) => {
            snap.instance.variables = Arc::new(vars);
            snap.instance.variables_spilled = false;
            Ok(())
        }
        None => {
            // Only a wired, readable tier's miss is a definitive absence.
            if (varstore.is_some() || spill_store.is_some())
                && let Some(absences) = absences
            {
                absences.record(key, nanobpmn_engine_core::OrphanKind::Resident);
            }
            Err(invalid_data(&format!(
                "spilled instance {key} has no variable row in any wired store"
            )))
        }
    }
}

/// Reads a combined multi-partition streaming snapshot back into a map from
/// global partition id to `(covered_events, EngineSnapshot)`, streaming each
/// instance chunk into its partition's skeleton as it arrives (#1463).
/// Fail-closed on a torn write (no terminal `end` frame).
pub fn read_multi_streaming_snapshot(
    path: &Path,
) -> std::io::Result<std::collections::HashMap<u64, (u64, EngineSnapshot)>> {
    read_multi_streaming_snapshot_based(path).map(|(map, _)| map)
}

/// Per-partition rebase-floor identities read from a combined snapshot's
/// control frames (#1405); a partition without one is absent.
pub type MultiBaseIds = std::collections::HashMap<u64, u64>;

/// [`read_multi_streaming_snapshot`] plus each partition's control-frame
/// rebase-floor identity.
#[allow(clippy::type_complexity)]
pub fn read_multi_streaming_snapshot_based(
    path: &Path,
) -> std::io::Result<(
    std::collections::HashMap<u64, (u64, EngineSnapshot)>,
    MultiBaseIds,
)> {
    let mut bases = MultiBaseIds::new();
    let file = File::open(path)?;
    let mut reader = BufReader::new(file);

    // Line 1: envelope header (validated by the caller).
    let mut header_line = String::new();
    if reader.read_line(&mut header_line)? == 0 {
        return Err(invalid_data(
            "multi streaming snapshot is empty (no header)",
        ));
    }

    let mut map: std::collections::HashMap<u64, (u64, EngineSnapshot)> =
        std::collections::HashMap::new();
    let mut saw_end = false;
    let mut line = String::new();
    loop {
        line.clear();
        if reader.read_line(&mut line)? == 0 {
            break;
        }
        let trimmed = line.trim();
        if trimmed.is_empty() {
            continue;
        }
        // Distinguish a control frame from a chunk frame by the presence of the
        // `control` key (control frames carry it; chunk frames carry
        // `instances`). Parse generically first.
        let value: serde_json::Value = serde_json::from_str(trimmed).map_err(into_invalid_data)?;
        if value.get("control").is_some() {
            let frame: MultiControlFrame =
                serde_json::from_value(value).map_err(into_invalid_data)?;
            if let Some(id) = frame.base_id {
                bases.insert(frame.partition, id);
            }
            map.insert(frame.partition, (frame.covered_events, frame.control));
        } else {
            let chunk: MultiInstanceChunk =
                serde_json::from_value(value).map_err(into_invalid_data)?;
            let end = chunk.end;
            if !chunk.instances.is_empty() {
                let entry = map.get_mut(&chunk.partition).ok_or_else(|| {
                    invalid_data(&format!(
                        "multi snapshot chunk for unknown partition {}",
                        chunk.partition
                    ))
                })?;
                for snap in chunk.instances {
                    nanobpmn_engine_core::restore_instance_snapshot(&mut entry.1.state, snap);
                }
            }
            if end {
                saw_end = true;
                break;
            }
        }
    }
    if !saw_end {
        return Err(invalid_data(
            "multi streaming snapshot is truncated (no terminal frame); refusing to \
             restore an incomplete instance set",
        ));
    }
    Ok((map, bases))
}

#[cfg(test)]
#[allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (unit tests)"
)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};

    use nanobpmn_engine_core::{
        Command, Engine, EngineSnapshot, InstanceSnapshot, ProcessBuilder, State, Value,
    };

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

    /// Builds an engine with `n` live instances, each carrying a `data` payload,
    /// parked on its `demo-work` job.
    fn engine_with_instances(n: usize) -> Engine {
        let mut engine = Engine::new();
        engine
            .apply_command(Command::DeployProcess(demo()))
            .expect("deploy");
        for i in 0..n {
            let mut vars = HashMap::new();
            vars.insert("data".to_string(), Value::Str(format!("payload-{i}")));
            engine
                .apply_command(Command::create_instance_with("demo", vars))
                .expect("create");
        }
        engine
    }

    fn tmp_path(tag: &str) -> std::path::PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "nanobpmn-snapstream-{tag}-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)
                .unwrap()
                .as_nanos()
        ));
        std::fs::create_dir_all(&dir).unwrap();
        dir.join("snapshot.stream.bin")
    }

    /// A normalized, order-independent view of an engine's instances + payloads,
    /// used to assert a streaming round-trip is lossless.
    fn instance_view(engine: &Engine) -> Vec<(Key, String)> {
        let mut v: Vec<(Key, String)> = engine
            .state()
            .instances
            .values()
            .map(|inst| {
                let data = match inst.variables.get("data") {
                    Some(Value::Str(s)) => s.clone(),
                    other => panic!("instance {} missing payload: {other:?}", inst.key),
                };
                (inst.key, data)
            })
            .collect();
        v.sort();
        v
    }

    fn header_line() -> String {
        // Minimal always-parseable envelope header (the real one is stamped by
        // seglog; only its single-line shape matters to the stream framing).
        "{\"format_version\":3,\"incarnation\":1,\"engine_fingerprint\":\"test\"}".to_string()
    }

    // #1405: the rebase-floor identity rides in the v3 control frame (single and
    // multi), additively — absent means `None`, present round-trips verbatim.
    #[test]
    fn control_frames_round_trip_base_id_additively() {
        let engine = engine_with_instances(3);
        for base_id in [None, Some(0xfeed_u64)] {
            let (control, instances) =
                Engine::decompose_state_for_streaming(engine.state().clone());
            let path = tmp_path("base-id");
            write_streaming_snapshot(StreamWrite {
                path: &path,
                header_line: &header_line(),
                covered_events: 5,
                base_id,
                control: control_snapshot(&control, &engine),
                instances,
                cold: &[],
                read_cold: |_key| Err(invalid_data("no cold tier in this test")),
                resolve_spilled: |_snap| Ok(()),
                control_only: false,
            })
            .expect("streaming write");
            let control_line = std::fs::read_to_string(&path).unwrap();
            let control_line = control_line.lines().nth(1).unwrap();
            assert_eq!(control_line.contains("\"base_id\""), base_id.is_some());
            let got = read_streaming_snapshot_based(&path, |_, _| {}, |_| {}).unwrap();
            assert_eq!(got, base_id);
            let _ = std::fs::remove_dir_all(path.parent().unwrap());

            let (control, resident) = Engine::decompose_state_for_streaming(engine.state().clone());
            let path = tmp_path("multi-base-id");
            let ids: MultiBaseIds = base_id.map(|id| (0, id)).into_iter().collect();
            write_multi_streaming_snapshot(
                &path,
                &header_line(),
                vec![(
                    0,
                    5,
                    StreamingCapture {
                        control: control_snapshot(&control, &engine),
                        resident,
                    },
                    Vec::new(),
                    SpillStores::default(),
                    false,
                )],
                &ids,
            )
            .expect("multi streaming write");
            let (map, bases) = read_multi_streaming_snapshot_based(&path).unwrap();
            assert_eq!(map.get(&0).map(|(c, _)| *c), Some(5));
            assert_eq!(bases.get(&0).copied(), base_id);
            let _ = std::fs::remove_dir_all(path.parent().unwrap());
        }
    }

    // #1337 red/green: a streaming snapshot must round-trip every resident
    // instance's payload WITHOUT materializing the whole set at once, and a
    // restore must rebuild an equivalent engine.
    #[test]
    fn streaming_snapshot_round_trips_all_instances() {
        let n = 200; // > STREAM_CHUNK_INSTANCES so multiple chunks are exercised.
        let engine = engine_with_instances(n);
        let expected = instance_view(&engine);

        let (control, instances) = Engine::decompose_state_for_streaming(engine.state().clone());
        assert!(
            control.instances.is_empty(),
            "control half carries no instances"
        );
        assert_eq!(instances.len(), n);
        // The control skeleton's derived job indices must be consistent with the
        // drained `jobs` map: no dangling index entries pointing at removed jobs
        // (Copilot review on #1464).
        assert!(
            control.jobs.is_empty(),
            "control half carries no per-instance jobs"
        );
        assert!(
            control.jobs_by_instance.is_empty(),
            "no jobs_by_instance entry may survive its instance's jobs"
        );
        assert!(
            control.activatable_jobs.values().all(|s| s.is_empty()),
            "no activatable index entry may point at a drained job"
        );
        assert!(
            control.activated_jobs.is_empty(),
            "no activated index entry may point at a drained job"
        );

        let path = tmp_path("roundtrip");
        write_streaming_snapshot(StreamWrite {
            path: &path,
            header_line: &header_line(),
            covered_events: 42,
            base_id: None,
            control: control_snapshot(&control, &engine),
            instances,
            cold: &[],
            read_cold: |_key| Err(invalid_data("no cold tier in this test")),
            resolve_spilled: |_snap| Ok(()),
            control_only: false,
        })
        .expect("streaming write");

        // Restore into a fresh engine, one instance at a time.
        let mut restored: Option<EngineSnapshot> = None;
        let mut restored_instances: Vec<InstanceSnapshot> = Vec::new();
        read_streaming_snapshot(
            &path,
            |control, covered| {
                assert_eq!(covered, 42);
                restored = Some(control);
            },
            |snap| restored_instances.push(snap),
        )
        .expect("streaming read");

        let mut control = restored.expect("control frame present");
        assert_eq!(restored_instances.len(), n);
        for snap in restored_instances {
            nanobpmn_engine_core::restore_instance_snapshot(&mut control.state, snap);
        }
        let rebuilt = Engine::from_snapshot(control);
        assert_eq!(instance_view(&rebuilt), expected);

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1337: the whole point — a wholly off-heap (cold) instance is streamed
    // straight from the spill store and NEVER becomes resident during capture.
    #[test]
    fn streaming_snapshot_reads_cold_instances_without_residency() {
        let engine = engine_with_instances(3);
        let (control, instances) = Engine::decompose_state_for_streaming(engine.state().clone());
        // Simulate "all three instances are cold": the resident decomposition is
        // empty, and the cold keys are served by a store closure.
        let cold_keys: Vec<Key> = instances.iter().map(|s| s.instance.key).collect();
        let cold_store: HashMap<Key, InstanceSnapshot> =
            instances.into_iter().map(|s| (s.instance.key, s)).collect();

        // The cold set must be read LAZILY — interleaved with the stream, not
        // all up front. Record the completion order of (read, flush) by counting
        // reads that have happened by the time each chunk is written. Because
        // STREAM_CHUNK_INSTANCES (64) exceeds the 3 cold keys, a lazy reader
        // reads all 3 before the single terminal flush; the meaningful assertion
        // here is that the reads happen through `read_cold` (one at a time, on
        // demand) rather than the cold set being folded into the resident
        // decomposition up front — which the empty `instances` argument proves.
        let reads = Arc::new(AtomicUsize::new(0));
        let reads2 = Arc::clone(&reads);

        let path = tmp_path("cold");
        write_streaming_snapshot(StreamWrite {
            path: &path,
            header_line: &header_line(),
            covered_events: 7,
            base_id: None,
            control: control_snapshot(&control, &engine),
            instances: Vec::new(), // nothing resident — the cold set is NOT folded in up front
            cold: &cold_keys,
            read_cold: move |key| {
                // Each cold row is read on demand, as the stream reaches it.
                reads2.fetch_add(1, Ordering::SeqCst);
                cold_store
                    .get(&key)
                    .cloned()
                    .ok_or_else(|| invalid_data("missing cold row"))
            },
            resolve_spilled: |_snap| Ok(()),
            control_only: false,
        })
        .expect("streaming write with cold tier");

        // Every cold row was read exactly once, through the lazy `read_cold`
        // path (never materialized into the resident set).
        assert_eq!(
            reads.load(Ordering::SeqCst),
            3,
            "each cold instance was read once, lazily, as it streamed out"
        );

        // Restore and confirm every cold instance came back.
        let mut restored: Option<EngineSnapshot> = None;
        let mut count = 0usize;
        read_streaming_snapshot(
            &path,
            |control, _| restored = Some(control),
            |_snap| count += 1,
        )
        .expect("streaming read");
        assert_eq!(count, 3, "every cold instance streamed back");
        assert!(restored.is_some());

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1337 + Copilot review on #1464: a COLD row can itself be variable-spilled
    // (spilled before it was cold-evicted). The cold streaming path must resolve
    // that spilled payload too, so the snapshot it writes is self-contained —
    // restoring it must yield the variables and a cleared `variables_spilled`
    // flag, not an empty placeholder still marked spilled.
    #[test]
    fn streaming_snapshot_resolves_spilled_cold_instances() {
        let engine = engine_with_instances(1);
        let (control, instances) = Engine::decompose_state_for_streaming(engine.state().clone());
        let mut cold_snap = instances.into_iter().next().expect("one instance");
        let key = cold_snap.instance.key;

        // Capture the real payload, then mark the cold row spilled (empty
        // placeholder variables + the spilled flag), as a cold-evicted row that
        // was variable-spilled first would look.
        let real_vars = (*cold_snap.instance.variables).clone();
        let payload = match real_vars.get("data") {
            Some(Value::Str(s)) => s.clone(),
            other => panic!("missing payload: {other:?}"),
        };
        cold_snap.instance.variables = Arc::new(HashMap::new());
        cold_snap.instance.variables_spilled = true;

        let cold_keys: Vec<Key> = vec![key];
        let resolved_vars = real_vars.clone();

        let path = tmp_path("cold-spilled");
        write_streaming_snapshot(StreamWrite {
            path: &path,
            header_line: &header_line(),
            covered_events: 9,
            base_id: None,
            control: control_snapshot(&control, &engine),
            instances: Vec::new(), // the instance is cold, not resident
            cold: &cold_keys,
            read_cold: move |k| {
                assert_eq!(k, key);
                Ok(cold_snap.clone())
            },
            // The writer must call this for the spilled cold row and rehydrate it.
            resolve_spilled: move |snap| {
                snap.instance.variables = Arc::new(resolved_vars.clone());
                snap.instance.variables_spilled = false;
                Ok(())
            },
            control_only: false,
        })
        .expect("streaming write of a spilled cold instance");

        // Restore: the single instance must come back with its variables present
        // and the spilled flag cleared (a self-contained snapshot).
        let mut restored_instances: Vec<InstanceSnapshot> = Vec::new();
        read_streaming_snapshot(
            &path,
            |_control, _covered| {},
            |snap| restored_instances.push(snap),
        )
        .expect("streaming read");

        assert_eq!(restored_instances.len(), 1);
        let snap = &restored_instances[0];
        assert!(!snap.instance.variables_spilled, "spilled flag cleared");
        match snap.instance.variables.get("data") {
            Some(Value::Str(s)) => assert_eq!(*s, payload),
            other => panic!("restored cold instance missing payload: {other:?}"),
        }

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1337 fail-closed: a torn stream (no terminal frame) must be rejected, not
    // silently restored as a prefix of the instances.
    #[test]
    fn streaming_snapshot_rejects_a_truncated_stream() {
        let engine = engine_with_instances(5);
        let (control, instances) = Engine::decompose_state_for_streaming(engine.state().clone());
        let path = tmp_path("truncated");
        write_streaming_snapshot(StreamWrite {
            path: &path,
            header_line: &header_line(),
            covered_events: 1,
            base_id: None,
            control: control_snapshot(&control, &engine),
            instances,
            cold: &[],
            read_cold: |_key| Err(invalid_data("no cold tier")),
            resolve_spilled: |_snap| Ok(()),
            control_only: false,
        })
        .expect("write");

        // Truncate away the terminal frame: drop the last non-empty line.
        let bytes = std::fs::read(&path).unwrap();
        let text = String::from_utf8(bytes).unwrap();
        let mut lines: Vec<&str> = text.lines().collect();
        // Remove trailing empty + the terminal frame line.
        while matches!(lines.last(), Some(l) if l.is_empty()) {
            lines.pop();
        }
        lines.pop(); // drop the terminal {"instances":[],"end":true}
        let truncated = lines.join("\n") + "\n";
        std::fs::write(&path, truncated).unwrap();

        let mut count = 0usize;
        let result = read_streaming_snapshot(&path, |_c, _cov| {}, |_s| count += 1);
        assert!(
            result.is_err(),
            "a truncated stream must fail closed, not restore a prefix"
        );

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// Builds the `EngineSnapshot` control value from a decomposed control state
    /// plus the originating engine's scalar metadata.
    fn control_snapshot(control: &State, engine: &Engine) -> EngineSnapshot {
        let full = engine.snapshot();
        EngineSnapshot {
            state: control.clone(),
            partition_id: full.partition_id,
            next_local: full.next_local,
            num_partitions: full.num_partitions,
            now: full.now,
            start_dispatch_rr: full.start_dispatch_rr,
            raft_applied: full.raft_applied,
        }
    }

    // #1464 review (legacy/upgrade compat, the #1065 replay class): a snapshot
    // written by a build PREDATING the chunked sidecar framing is ONE headerless
    // `EngineSnapshot` JSON value (no envelope header line, no control/chunk
    // frames). The reader must detect that legacy shape and restore it — not
    // mistake its first line for a header and fail with "no control frame",
    // which would break boot recovery / install of a durable pre-upgrade
    // snapshot. Red-first: this fails before the legacy fallback was added.
    #[test]
    fn streaming_snapshot_reads_a_legacy_headerless_snapshot() {
        let engine = engine_with_instances(4);
        let expected = instance_view(&engine);

        // Serialize the WHOLE snapshot as one bare JSON value — the pre-#1337
        // on-disk layout (headerless, unframed). This is exactly what
        // `serde_json::from_reader` used to decode.
        let legacy = engine.snapshot();
        let body = serde_json::to_string(&legacy).expect("serialize legacy snapshot");
        assert!(
            !body.trim_start().starts_with("{\"format_version\""),
            "legacy body must NOT carry an envelope header"
        );
        let path = tmp_path("legacy-headerless");
        std::fs::write(&path, body).unwrap();

        // The streaming reader must restore the legacy file transparently.
        let rebuilt_snapshot =
            read_streaming_engine_snapshot(&path).expect("legacy headerless snapshot must restore");
        let rebuilt = Engine::from_snapshot(rebuilt_snapshot);
        assert_eq!(
            instance_view(&rebuilt),
            expected,
            "a legacy headerless snapshot restores every instance"
        );

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1464 review (fail-closed spill resolution): when the authoritative var
    // store has NO row for a spilled instance but a LEGACY spill row still exists
    // (an unmigrated lean-over-legacy transition), the streaming writer must FAIL
    // CLOSED rather than seal the potentially-stale legacy payload into a
    // self-contained snapshot — mirroring `Journal::peek_spilled_variables`,
    // which refuses exactly this state. Red-first: the writer previously fell
    // back to the legacy row and cleared the spilled flag.
    #[test]
    fn streaming_writer_rejects_a_stale_legacy_spill_row() {
        let engine = engine_with_instances(1);
        let (control, instances) = Engine::decompose_state_for_streaming(engine.state().clone());
        let mut snap = instances.into_iter().next().expect("one instance");
        let key = snap.instance.key;

        // Mark the resident instance variable-spilled (empty placeholder + flag).
        let stale_vars = (*snap.instance.variables).clone();
        snap.instance.variables = Arc::new(HashMap::new());
        snap.instance.variables_spilled = true;

        // Stores: a lean var store with NO row for `key`, and a legacy spill
        // store that STILL has a (stale) row. The production
        // `write_streaming_snapshot_from_stores` resolve_spilled must refuse
        // this, not seal the stale row.
        let varstore = Arc::new(VarStore::open(None).expect("var store"));
        let spill = Arc::new(VarSpillStore::open(None).expect("spill store"));
        spill.put(key, &stale_vars).expect("seed legacy spill row");

        let path = tmp_path("stale-legacy-spill");
        let result = write_streaming_snapshot_from_stores(
            &path,
            &header_line(),
            7,
            None,
            StreamingCapture {
                control: control_snapshot(&control, &engine),
                resident: vec![snap],
            },
            &[],
            SpillStores {
                cold: None,
                spill: Some(spill),
                varstore: Some(varstore),
                legacy: None,
                absences: None,
            },
            false,
        );
        assert!(
            result.is_err(),
            "a var-store miss with a surviving legacy spill row must fail closed, \
             not seal a stale payload"
        );
        // Fail-closed also means the partial snapshot is removed, never sealed.
        assert!(
            !path.exists(),
            "the rejected snapshot's partial file must be removed"
        );

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    // #1574 review: with a lean var store wired but the legacy SPILL store
    // absent, a var-store miss for a spilled instance must consult the COLD tier
    // (spill-or-cold, like `Journal::peek_spilled_variables`) before declaring a
    // definitive absence. A cold-shed instance's payload lives in its cold row,
    // so a cold-only configuration must not record an orphan / must fail closed.
    #[test]
    fn streaming_writer_checks_the_cold_tier_before_declaring_a_loss() {
        let engine = engine_with_instances(1);
        let (control, instances) = Engine::decompose_state_for_streaming(engine.state().clone());
        let mut snap = instances.into_iter().next().expect("one instance");
        let key = snap.instance.key;

        // Mark the resident instance variable-spilled (empty placeholder + flag).
        snap.instance.variables = Arc::new(HashMap::new());
        snap.instance.variables_spilled = true;

        // Stores: a lean var store with NO row for `key`, NO legacy spill store,
        // and a COLD store that DOES hold the instance's row (payload inline).
        let varstore = Arc::new(VarStore::open(None).expect("var store"));
        let cold = Arc::new(VarSpillStore::open(None).expect("cold store"));
        cold.put_cold(key, &snap).expect("seed cold row");
        let absences = crate::journal::OrphanInbox::default();

        let path = tmp_path("cold-tier-1574");
        let result = write_streaming_snapshot_from_stores(
            &path,
            &header_line(),
            7,
            None,
            StreamingCapture {
                control: control_snapshot(&control, &engine),
                resident: vec![snap],
            },
            &[],
            SpillStores {
                cold: None,
                spill: None,
                varstore: Some(varstore),
                legacy: Some(cold),
                absences: Some(absences.clone()),
            },
            false,
        );
        assert!(
            result.is_err(),
            "a var-store miss with a surviving cold row must fail closed, not seal"
        );
        // Crucially the cold row's presence means NO definitive absence is
        // recorded — the payload is recoverable, so it must not be quarantined.
        assert!(
            !absences.contains(key),
            "no orphan recorded while the cold tier holds the payload"
        );

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
