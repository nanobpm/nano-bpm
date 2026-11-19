//! The determinism-CI **corpus runner** (ADR 0066 slice 6, #1665).
//!
//! Re-executes a recorded command corpus (JSON lines of
//! `{"now": u64, "command": Command}`) through `Engine::apply_command_at` and
//! writes one canonical JSON record line per corpus entry:
//!
//! * `{"i":N,"events":[…]}` — the events entry `N` emitted;
//! * `{"i":N,"err":"…"}` — the typed rejection entry `N` produced;
//! * `{"i":N,"state":{…}}` — the full `EngineSnapshot` after entry `N`, at
//!   every `--state-every` checkpoint (the canonical durable-state digest
//!   input; snapshot encoding is canonical since slice 3, #1662);
//! * `{"i":N,"undecodable":"…"}` — this build cannot decode entry `N` (a
//!   command variant newer than the build); the run stops there.
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

use nanobpmn_engine_core::{Command, Engine, EngineSnapshot};

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
}

/// One decoded corpus entry. Undecodable lines are kept as their error so
/// the runner can report exactly where an older build stops.
pub enum Entry {
    Ok { now: u64, command: Command },
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
        Ok(w) => Entry::Ok {
            now: w.now,
            command: w.command,
        },
        Err(e) => Entry::Undecodable(e.to_string()),
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
    if opts.canary {
        canary_permute(&mut entries);
    }
    let end = opts
        .stop_after
        .unwrap_or(entries.len() as u64)
        .min(entries.len() as u64);
    let mut i = opts.start_at;
    for entry in entries
        .into_iter()
        .skip(opts.start_at as usize)
        .take(end.saturating_sub(opts.start_at) as usize)
    {
        let (now, command) = match entry {
            Entry::Ok { now, command } => (now, command),
            Entry::Undecodable(e) => {
                writeln!(out, "{}", serde_json::json!({ "i": i, "undecodable": e }))?;
                return Ok(i);
            }
        };
        let record = match engine.apply_command_at(command, now) {
            Ok(events) => serde_json::json!({ "i": i, "events": events }),
            Err(e) => serde_json::json!({ "i": i, "err": e.to_string() }),
        };
        writeln!(out, "{record}")?;
        i += 1;
        if opts.state_every > 0 && i % opts.state_every == 0 {
            let state = serde_json::to_value(engine.snapshot())
                .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;
            writeln!(out, "{}", serde_json::json!({ "i": i - 1, "state": state }))?;
        }
    }
    Ok(i)
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
            matches!(
                e,
                Entry::Ok {
                    command: Command::CreateInstance { .. },
                    ..
                }
            )
        })
        .map(|(i, _)| i)
        .collect();
    let mut by_hash: std::collections::HashMap<usize, Command> = std::collections::HashMap::new();
    for &p in &positions {
        if let Entry::Ok { command, .. } = &entries[p] {
            by_hash.insert(p, command.clone());
        }
    }
    // HashMap iteration order depends on the per-process random hash seed.
    let shuffled: Vec<Command> = by_hash.into_values().collect();
    for (&p, command) in positions.iter().zip(shuffled) {
        if let Entry::Ok { command: slot, .. } = &mut entries[p] {
            *slot = command;
        }
    }
}
