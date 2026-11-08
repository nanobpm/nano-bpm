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
use std::io::{BufRead, BufReader, BufWriter, Write};
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
    pub control: EngineSnapshot,
    pub instances: I,
    pub cold: &'a [Key],
    pub read_cold: RC,
    pub resolve_spilled: RS,
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
        control,
        instances,
        cold,
        mut read_cold,
        mut resolve_spilled,
    } = req;
    let tmp = path.with_extension("tmp");
    let outcome = write_streaming_snapshot_inner(
        &tmp,
        header_line,
        covered_events,
        control,
        instances,
        cold,
        &mut read_cold,
        &mut resolve_spilled,
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
    control: EngineSnapshot,
    instances: I,
    cold: &[Key],
    read_cold: &mut RC,
    resolve_spilled: &mut RS,
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
    // then stream it out. Consuming `instances` by value drops each payload as
    // its chunk flushes.
    for mut snap in instances {
        if snap.instance.variables_spilled {
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
    // keep the snapshot self-contained.
    for &key in cold {
        let mut snap = read_cold(key)?;
        if snap.instance.variables_spilled {
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
/// Purely synchronous and owning only `Arc` store handles — safe to call from a
/// blocking task off the engine actor. Fail-closed: any unreadable/absent row
/// aborts the write and removes the partial file.
pub fn write_streaming_snapshot_from_stores(
    path: &Path,
    header_line: &str,
    covered_events: u64,
    capture: StreamingCapture,
    cold_keys: &[Key],
    stores: SpillStores,
) -> std::io::Result<()> {
    let SpillStores {
        cold: cold_store,
        spill: spill_store,
        varstore,
    } = stores;
    write_streaming_snapshot(StreamWrite {
        path,
        header_line,
        covered_events,
        control: capture.control,
        instances: capture.resident,
        cold: cold_keys,
        read_cold: move |key| {
            let store = cold_store.as_ref().ok_or_else(|| {
                invalid_data("cold index references a key but no cold store is wired")
            })?;
            store
                .try_get_cold(key)
                .map_err(|e| invalid_data(&e.to_string()))?
                .ok_or_else(|| {
                    invalid_data(&format!(
                        "cold instance {key} has no row in the spill store"
                    ))
                })
        },
        resolve_spilled: move |snap| {
            let key = snap.instance.key;
            // Lean mode reads the authoritative var store; classic reads the
            // spill cache. Mirror `Journal::peek_spilled_variables`'s preference.
            // Fail-closed throughout: a store read ERROR is propagated (never
            // swallowed into a silent `None`), so a transient/corrupt row aborts
            // the snapshot instead of being mistaken for a definitively-absent
            // payload.
            let resolved = if let Some(vs) = varstore.as_ref() {
                match vs.try_get(key).map_err(|e| invalid_data(&e.to_string()))? {
                    Some(vars) => Some(vars),
                    // Var store has no row: fall back to a legacy spill row, but
                    // propagate a spill-store read error rather than flattening
                    // it into "absent".
                    None => match spill_store.as_ref() {
                        Some(s) => s.try_get(key).map_err(|e| invalid_data(&e.to_string()))?,
                        None => None,
                    },
                }
            } else if let Some(sp) = spill_store.as_ref() {
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
                None => Err(invalid_data(&format!(
                    "spilled instance {key} has no variable row in any wired store"
                ))),
            }
        },
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
    mut restore_control: impl FnMut(EngineSnapshot, u64),
    mut restore: R,
) -> std::io::Result<()>
where
    RD: std::io::Read,
    R: FnMut(InstanceSnapshot),
{
    let mut reader = BufReader::new(reader);

    // Line 1: the envelope header (its format version is validated by the caller
    // before this is invoked). The line is consumed to advance past it; the body
    // is not inspected here.
    let mut _header_line = String::new();
    if reader.read_line(&mut _header_line)? == 0 {
        return Err(invalid_data("streaming snapshot is empty (no header)"));
    }

    // Line 2: the control frame.
    let mut control_line = String::new();
    if reader.read_line(&mut control_line)? == 0 {
        return Err(invalid_data("streaming snapshot has no control frame"));
    }
    let control_frame: ControlFrame =
        serde_json::from_str(&control_line).map_err(into_invalid_data)?;
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
    Ok(())
}

fn into_invalid_data(e: serde_json::Error) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, e)
}

fn invalid_data(msg: &str) -> std::io::Error {
    std::io::Error::new(std::io::ErrorKind::InvalidData, msg.to_string())
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
            control: control_snapshot(&control, &engine),
            instances,
            cold: &[],
            read_cold: |_key| Err(invalid_data("no cold tier in this test")),
            resolve_spilled: |_snap| Ok(()),
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
            control: control_snapshot(&control, &engine),
            instances,
            cold: &[],
            read_cold: |_key| Err(invalid_data("no cold tier")),
            resolve_spilled: |_snap| Ok(()),
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
        }
    }
}
