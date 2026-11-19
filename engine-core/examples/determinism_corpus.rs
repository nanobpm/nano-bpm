//! Determinism-CI corpus tool (ADR 0066 slice 6, #1665).
//!
//! ```bash
//! # write the command corpus (every Command variant + a seeded workload)
//! cargo run --features serde --example determinism_corpus -- emit CORPUS [--random N]
//! # compare head records with a previous release's, through the head decoder
//! cargo run --features serde --example determinism_corpus -- cross-release HEAD PREV
//! ```
//!
//! `cross-release` re-decodes both record files' events through the **current**
//! `Event` type and re-encodes them (so an additive `#[serde(default)]` field —
//! replay-compatible by the event-frame rules — is not a false divergence),
//! compares rejections by presence only (error wording is not replicated
//! state), ignores `state` checkpoints (the snapshot shape is versioned
//! separately, `SNAPSHOT_FORMAT_VERSION`), and checks every entry up to the
//! first one the previous release could not decode.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "non-production target (integration tests / examples); CLI argument parsing is not an apply path (#1663)"
)]

#[path = "determinism/corpus.rs"]
mod corpus;

use serde_json::Value as Json;

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("emit") => {
            let path = args.get(1).expect("emit PATH");
            let random = match args.get(2).map(String::as_str) {
                Some("--random") => args.get(3).expect("--random N").parse().expect("N"),
                _ => 400,
            };
            let entries = corpus::build(random);
            std::fs::write(path, corpus::to_jsonl(&entries)).expect("write corpus");
            eprintln!(
                "determinism_corpus: {} entries, {} command kinds",
                entries.len(),
                corpus::covered_kinds(&entries).len()
            );
        }
        Some("cross-release") => {
            let head = read(args.get(1).expect("HEAD"));
            let prev = read(args.get(2).expect("PREV"));
            match cross_release(&head, &prev) {
                Ok(n) => eprintln!("determinism_corpus: {n} entries identical to previous release"),
                Err(e) => {
                    eprintln!("determinism_corpus: DIVERGENCE vs previous release: {e}");
                    std::process::exit(1);
                }
            }
        }
        _ => {
            eprintln!("usage: determinism_corpus emit PATH [--random N] | cross-release HEAD PREV");
            std::process::exit(2);
        }
    }
}

fn read(path: &str) -> Vec<Json> {
    std::fs::read_to_string(path)
        .expect("records")
        .lines()
        .filter(|l| !l.trim().is_empty())
        .map(|l| serde_json::from_str(l).expect("record line"))
        .filter(|r: &Json| r.get("state").is_none())
        .collect()
}

/// A record in cross-release canonical form, or `None` past the comparable
/// prefix (an `undecodable` marker).
fn canonical(record: &Json) -> Result<Option<String>, String> {
    if record.get("undecodable").is_some() {
        return Ok(None);
    }
    if record.get("err").is_some() {
        return Ok(Some("err".into()));
    }
    let events = record
        .get("events")
        .and_then(Json::as_array)
        .ok_or_else(|| format!("malformed record {record}"))?;
    let mut out = Vec::with_capacity(events.len());
    for e in events {
        let event = nanobpmn_engine_core::decode_event_json(&e.to_string())
            .map_err(|err| format!("event does not decode under head: {err}: {e}"))?;
        out.push(serde_json::to_string(&event).map_err(|err| err.to_string())?);
    }
    Ok(Some(out.join("\n")))
}

fn cross_release(head: &[Json], prev: &[Json]) -> Result<usize, String> {
    let mut compared = 0;
    for (i, p) in prev.iter().enumerate() {
        let Some(p) = canonical(p)? else { break };
        let h = head
            .get(i)
            .ok_or_else(|| format!("head has no record {i}"))
            .and_then(canonical)?
            .ok_or_else(|| format!("head could not decode entry {i}"))?;
        if h != p {
            return Err(format!("entry {i}:\n  head: {h}\n  prev: {p}"));
        }
        compared += 1;
    }
    if compared == 0 {
        return Err("no comparable entries (previous release decoded nothing)".into());
    }
    Ok(compared)
}
