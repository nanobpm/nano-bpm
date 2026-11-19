//! Determinism-CI corpus tool (ADR 0066 slice 6, #1665).
//!
//! ```bash
//! # write the command corpus (every Command variant + a seeded workload)
//! cargo run --features serde --example determinism_corpus -- emit CORPUS [--random N]
//! # regenerate it without the entries a previous release's probe run marked
//! # undecodable (keys re-harvested; fails if a kept entry's outcome changes)
//! cargo run --features serde --example determinism_corpus -- emit-compatible PROBE OUT [--random N]
//! # compare head records with a previous release's, through the head decoder
//! cargo run --features serde --example determinism_corpus -- cross-release HEAD PREV
//! ```
//!
//! `cross-release` re-decodes both record files' events through the **current**
//! `Event` type and re-encodes them (so an additive `#[serde(default)]` field —
//! replay-compatible by the event-frame rules — is not a false divergence),
//! compares rejections by presence only (error wording is not replicated
//! state) and compares `state` checkpoints through the head `EngineSnapshot`
//! type whenever both builds share a `SNAPSHOT_FORMAT_VERSION` (see
//! `determinism/compare.rs`). Both record files must come from the
//! `emit-compatible` corpus, so every entry is compared.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::disallowed_methods,
    reason = "non-production target (integration tests / examples); CLI argument parsing is not an apply path (#1663)"
)]

#[path = "determinism/compare.rs"]
mod compare;
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
        Some("emit-compatible") => {
            // Regenerate the corpus without the entries a previous release's
            // probe run marked undecodable, harvesting keys from the filtered
            // engine (never dropping lines from the emitted file, which would
            // leave later commands pointing at keys that no longer exist).
            let skip = compare::undecodable_indices(&read(args.get(1).expect("PROBE")));
            let path = args.get(2).expect("OUT");
            let random = match args.get(3).map(String::as_str) {
                Some("--random") => args.get(4).expect("--random N").parse().expect("N"),
                _ => 400,
            };
            match corpus::build_excluding(random, skip.clone()) {
                Ok((entries, excluded)) => {
                    std::fs::write(path, corpus::to_jsonl(&entries)).expect("write corpus");
                    eprintln!(
                        "determinism_corpus: excluded {} undecodable entries + {} dependents \
                         {excluded:?}; {} kept",
                        skip.len(),
                        excluded.len() - skip.len(),
                        entries.len()
                    );
                }
                Err(e) => {
                    eprintln!("determinism_corpus: EXCLUSIONS DEGRADE THE CORPUS: {e}");
                    std::process::exit(1);
                }
            }
        }
        Some("cross-release") => {
            let head = read(args.get(1).expect("HEAD"));
            let prev = read(args.get(2).expect("PREV"));
            match compare::cross_release(&head, &prev) {
                Ok(r) => eprintln!(
                    "determinism_corpus: {} entries identical to previous release; \
                     {} state checkpoints identical, {} skipped (snapshot format bump)",
                    r.entries, r.states_compared, r.states_skipped_format_bump
                ),
                Err(e) => {
                    eprintln!("determinism_corpus: DIVERGENCE vs previous release: {e}");
                    std::process::exit(1);
                }
            }
        }
        _ => {
            eprintln!(
                "usage: determinism_corpus emit PATH [--random N] | emit-compatible PROBE OUT [--random N] | cross-release HEAD PREV"
            );
            std::process::exit(2);
        }
    }
}

fn read(path: &str) -> Vec<Json> {
    compare::parse_records(&std::fs::read_to_string(path).expect("records")).expect("records")
}
