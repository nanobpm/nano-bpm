//! The determinism-CI **corpus runner** (ADR 0066 slice 6, #1665).
//!
//! Re-executes a recorded command corpus (JSON lines of
//! `{"now": u64, "command": Command}`) through `Engine::apply_command_at` and
//! writes one canonical JSON record line per corpus entry:
//!
//! * `{"i":N,"events":[…]}` — the events entry `N` emitted;
//! * `{"i":N,"err":"…"}` — the typed rejection entry `N` produced;
//! * `{"i":N,"format":V,"state":{…}}` — the full `EngineSnapshot` after
//!   entry `N` (`V` = this build's `SNAPSHOT_FORMAT_VERSION`), at every
//!   `--state-every` checkpoint **and** after the corpus's last entry (so the
//!   trailing partial interval is state-compared too; a `--stop-after` split
//!   that ends before the corpus does emits no extra checkpoint, keeping a
//!   split run byte-identical to an uninterrupted one). The canonical
//!   durable-state digest input; snapshot encoding is canonical since slice 3,
//!   #1662;
//! * `{"i":N,"undecodable":"…"}` — this build cannot decode entry `N` (a
//!   command variant newer than the build); the run stops there, or with
//!   `--skip-undecodable` continues without applying it. With
//!   `--skip-undecodable` an entry this build decodes **lossily** — a field
//!   the corpus sets (to a non-empty value) that this build's `Command` type
//!   silently ignores (an additive command field newer than the build) — is
//!   reported the same way (`"lossy decode: drops field `Variant.field`"`)
//!   instead of being applied as a different command (#1698).
//!
//! The CI job hashes these files and compares them across hash seeds, process
//! restarts, build profiles, platforms and the previous release.
//!
//! **Deliberately self-contained and API-minimal** (`Engine::new`,
//! `apply_command_at`, `snapshot`/`from_snapshot`, serde): the
//! previous-release cell copies this file into the previous release's source
//! tree and builds it there, so it must keep compiling against old releases.
//! Do not reach for newer engine API here.

use std::io::{BufRead, Write};

use nanobpmn_engine_core::{Command, Engine, EngineSnapshot, SNAPSHOT_FORMAT_VERSION};

/// Options for one runner invocation.
#[derive(Default)]
pub struct RunOptions {
    /// Emit a `state` record after every `state_every`-th entry (0 = never).
    pub state_every: u64,
    /// Stop after this many entries (`None` = run to the end).
    pub stop_after: Option<u64>,
    /// Start at this corpus index (entries before it are skipped — they are
    /// already folded into the `resume` snapshot).
    pub start_at: u64,
    /// Seeded test-only non-determinism (the red half of the gate's own
    /// red/green test, #1665): permutes the corpus's `CreateInstance` entries
    /// in `HashMap` iteration order, which varies with the per-process hash
    /// seed and reaches the emitted events (instance keys, variables).
    pub canary: bool,
    /// Record an undecodable entry and continue instead of stopping. The
    /// previous-release cell uses this once to learn which entries the old
    /// build cannot decode, then both builds replay the corpus without them.
    pub skip_undecodable: bool,
}

/// One decoded corpus entry. Undecodable lines are kept as their error so
/// the runner can report exactly where an older build stops.
pub enum Entry {
    Ok {
        now: u64,
        command: Box<Command>,
        /// Fields of the corpus line this build's decode dropped
        /// (`Variant.field` shape tokens) — whatever value they hold: an
        /// explicit `false`/`0`/`""` against a non-empty HEAD default is a
        /// behavioural difference, so emptiness is classified later by
        /// `lossy_is_benign`'s default-aware round trip, never here (#1698).
        lost: Vec<String>,
    },
    Undecodable(String),
}

/// Parses a corpus file's lines.
pub fn read_corpus(reader: impl BufRead) -> std::io::Result<Vec<Entry>> {
    let mut out = Vec::new();
    for line in reader.lines() {
        let line = line?;
        if line.trim().is_empty() {
            continue;
        }
        out.push(decode_entry(&line));
    }
    Ok(out)
}

fn decode_entry(line: &str) -> Entry {
    #[derive(serde::Deserialize)]
    struct Wire {
        now: u64,
        command: Command,
    }
    match serde_json::from_str::<Wire>(line) {
        Ok(w) => {
            let raw = serde_json::from_str::<serde_json::Value>(line)
                .ok()
                .and_then(|v| v.get("command").cloned())
                .unwrap_or_default();
            let decoded = serde_json::to_value(&w.command).unwrap_or_default();
            let mut lost = Vec::new();
            lost_fields(&raw, &decoded, "$", &mut lost);
            Entry::Ok {
                now: w.now,
                command: Box::new(w.command),
                lost,
            }
        }
        Err(e) => Entry::Undecodable(e.to_string()),
    }
}

fn is_field_name(key: &str) -> bool {
    key.starts_with(|c: char| c.is_ascii_lowercase())
        && key
            .chars()
            .all(|c| c.is_ascii_lowercase() || c.is_ascii_digit() || c == '_')
}

/// `raw` fields absent from `decoded` (the same value after a decode +
/// re-encode round trip), as `Variant.field` tokens. Every missing field is
/// reported, empty-valued ones included: whether ignoring a field is benign
/// depends on the HEAD default (`lossy_is_benign`'s default-aware round trip
/// through the head `Command` type), which a value-side check cannot see —
/// an explicit `false` against a default-`true` field (e.g.
/// `MultiInstance.cancel_remaining_instances`) is a behavioural difference.
fn lost_fields(
    raw: &serde_json::Value,
    decoded: &serde_json::Value,
    ctx: &str,
    out: &mut Vec<String>,
) {
    use serde_json::Value as J;
    match (raw, decoded) {
        (J::Object(r), J::Object(d)) => {
            for (k, rv) in r {
                let variant = k.starts_with(|c: char| c.is_ascii_uppercase());
                let next = if variant { k.as_str() } else { ctx };
                match d.get(k) {
                    Some(dv) => lost_fields(rv, dv, next, out),
                    None => out.push(if variant || !is_field_name(k) {
                        k.clone()
                    } else {
                        format!("{ctx}.{k}")
                    }),
                }
            }
        }
        (J::Array(r), J::Array(d)) => {
            for (rv, dv) in r.iter().zip(d) {
                lost_fields(rv, dv, ctx, out);
            }
        }
        _ => {}
    }
}

/// Applies `entries` to `engine`, writing one record line per entry to `out`.
/// Returns the index one past the last applied entry.
pub fn run(
    engine: &mut Engine,
    mut entries: Vec<Entry>,
    opts: &RunOptions,
    out: &mut dyn Write,
) -> std::io::Result<u64> {
    // The corpus replays PRE-LATCH (#2004): a feature-gated behaviour runs
    // only once every member advertises it, so the previous-release
    // comparison is a mixed-version cluster, where head must execute exactly
    // as the previous release. The previous release has no such switch; the
    // CI script strips `determinism:head-only` lines from its copy.
    engine.set_event_gateway_runtime(false); // determinism:head-only
    if opts.canary {
        canary_permute(&mut entries);
    }
    let total = entries.len() as u64;
    let end = opts.stop_after.unwrap_or(total).min(total);
    let mut i = opts.start_at;
    for entry in entries
        .into_iter()
        .skip(opts.start_at as usize)
        .take(end.saturating_sub(opts.start_at) as usize)
    {
        let (now, command) = match entry {
            Entry::Ok { lost, .. } if opts.skip_undecodable && !lost.is_empty() => {
                let fields: Vec<String> = lost.iter().map(|t| format!("`{t}`")).collect();
                let e = format!(
                    "lossy decode: drops field {}",
                    fields.join(", drops field ")
                );
                writeln!(out, "{}", serde_json::json!({ "i": i, "undecodable": e }))?;
                i += 1;
                continue;
            }
            Entry::Ok { now, command, .. } => (now, command),
            Entry::Undecodable(e) => {
                writeln!(out, "{}", serde_json::json!({ "i": i, "undecodable": e }))?;
                if !opts.skip_undecodable {
                    return Ok(i);
                }
                i += 1;
                continue;
            }
        };
        let record = match engine.apply_command_at(*command, now) {
            Ok(events) => serde_json::json!({ "i": i, "events": events }),
            Err(e) => serde_json::json!({ "i": i, "err": e.to_string() }),
        };
        writeln!(out, "{record}")?;
        i += 1;
        let at_cadence = i.is_multiple_of(opts.state_every);
        let at_corpus_end = i == total;
        if opts.state_every > 0 && (at_cadence || at_corpus_end) {
            write_state(engine, i - 1, out)?;
        }
    }
    Ok(i)
}

fn write_state(engine: &Engine, i: u64, out: &mut dyn Write) -> std::io::Result<()> {
    let state = serde_json::to_value(engine.snapshot())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    writeln!(
        out,
        "{}",
        serde_json::json!({ "i": i, "format": SNAPSHOT_FORMAT_VERSION, "state": state })
    )
}

/// Serializes the engine for a process restart.
pub fn save_snapshot(engine: &Engine) -> std::io::Result<Vec<u8>> {
    serde_json::to_vec(&engine.snapshot())
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))
}

/// Restores an engine saved by [`save_snapshot`] (a fresh process).
pub fn load_snapshot(bytes: &[u8]) -> std::io::Result<Engine> {
    let snapshot: EngineSnapshot = serde_json::from_slice(bytes)
        .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
    Ok(Engine::from_snapshot(snapshot))
}

#[allow(
    clippy::disallowed_types,
    reason = "the canary IS a deliberate HashMap-iteration non-determinism (#1665 red test); never enabled by the gate's real cells"
)]
fn canary_permute(entries: &mut [Entry]) {
    let positions: Vec<usize> = entries
        .iter()
        .enumerate()
        .filter(|(_, e)| {
            matches!(e, Entry::Ok { command, .. } if matches!(**command, Command::CreateInstance { .. }))
        })
        .map(|(i, _)| i)
        .collect();
    let mut by_hash: std::collections::HashMap<usize, Command> = std::collections::HashMap::new();
    for &p in &positions {
        if let Entry::Ok { command, .. } = &entries[p] {
            by_hash.insert(p, (**command).clone());
        }
    }
    // HashMap iteration order depends on the per-process random hash seed.
    let shuffled: Vec<Command> = by_hash.into_values().collect();
    for (&p, command) in positions.iter().zip(shuffled) {
        if let Entry::Ok { command: slot, .. } = &mut entries[p] {
            **slot = command;
        }
    }
}
