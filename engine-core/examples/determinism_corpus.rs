//! Determinism-CI corpus tool (ADR 0066 slice 6, #1665).
//!
//! ```bash
//! # write the command corpus (every Command variant + a seeded workload)
//! cargo run --features serde --example determinism_corpus -- emit CORPUS [--random N]
//! # drop the entries a previous release's probe run marked undecodable
//! cargo run --features serde --example determinism_corpus -- filter CORPUS PROBE OUT
//! # compare head records with a previous release's, through the head decoder
//! cargo run --features serde --example determinism_corpus -- cross-release HEAD PREV
//! ```
//!
//! `cross-release` re-decodes both record files' events through the **current**
//! `Event` type and re-encodes them (so an additive `#[serde(default)]` field —
//! replay-compatible by the event-frame rules — is not a false divergence),
//! compares rejections by presence only (error wording is not replicated
//! state) and ignores `state` checkpoints (the snapshot shape is versioned
//! separately, `SNAPSHOT_FORMAT_VERSION`). Both record files must come from
//! the `filter`ed corpus, so every entry is compared.

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
        Some("filter") => {
            let corpus = std::fs::read_to_string(args.get(1).expect("CORPUS")).expect("corpus");
            let probe = read(args.get(2).expect("PROBE"));
            let skip: std::collections::BTreeSet<u64> = probe
                .iter()
                .filter(|r| r.get("undecodable").is_some())
                .filter_map(|r| r.get("i").and_then(Json::as_u64))
                .collect();
            let kept: String = corpus
                .lines()
                .filter(|l| !l.trim().is_empty())
                .enumerate()
                .filter(|(i, _)| !skip.contains(&(*i as u64)))
                .map(|(_, l)| format!("{l}\n"))
                .collect();
            std::fs::write(args.get(3).expect("OUT"), kept).expect("write");
            eprintln!(
                "determinism_corpus: dropped {} entries the previous release cannot decode",
                skip.len()
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
            eprintln!(
                "usage: determinism_corpus emit PATH [--random N] | filter CORPUS PROBE OUT | cross-release HEAD PREV"
            );
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

/// A record in cross-release canonical form.
fn canonical(record: &Json) -> Result<String, String> {
    if let Some(e) = record.get("undecodable") {
        return Err(format!("undecodable entry in a filtered corpus: {e}"));
    }
    if record.get("err").is_some() {
        return Ok("err".into());
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
    Ok(out.join("\n"))
}

fn cross_release(head: &[Json], prev: &[Json]) -> Result<usize, String> {
    if head.len() != prev.len() {
        return Err(format!(
            "record counts differ: head {} vs prev {}",
            head.len(),
            prev.len()
        ));
    }
    if prev.is_empty() {
        return Err("no comparable entries".into());
    }
    for (i, (h, p)) in head.iter().zip(prev).enumerate() {
        let (h, p) = (canonical(h)?, canonical(p)?);
        if h != p {
            return Err(format!("entry {i}:\n  head: {h}\n  prev: {p}"));
        }
    }
    Ok(prev.len())
}
