//! Determinism-CI corpus tool (ADR 0066 slice 6, #1665).
//!
//! ```bash
//! # write the command corpus (every Command variant + a seeded workload)
//! cargo run --features serde --example determinism_corpus -- emit CORPUS [--random N]
//! # the post-latch corpus (#2004), for the same-release cells; replay it with
//! # `determinism_replay --event-gateway-runtime`
//! cargo run --features serde --example determinism_corpus -- emit CORPUS --post-latch
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

/// `--flag VALUE` lookup in `args`.
fn flag<'a>(args: &'a [String], name: &str) -> Option<&'a str> {
    args.iter().position(|a| a == name).map(|i| {
        args.get(i + 1)
            .unwrap_or_else(|| panic!("{name} needs a value"))
            .as_str()
    })
}

fn required<'a>(args: &'a [String], name: &str) -> &'a str {
    flag(args, name).unwrap_or_else(|| panic!("{name} is required"))
}

fn random(args: &[String]) -> usize {
    flag(args, "--random").map_or(400, |n| n.parse().expect("--random N"))
}

/// `vocabulary(golden @ --golden-head) − vocabulary(golden @ --golden-prev)`.
fn golden_added(args: &[String]) -> compare::Vocab {
    let vocab = |name| {
        compare::golden_vocab(std::path::Path::new(required(args, name))).unwrap_or_else(|e| {
            eprintln!("determinism_corpus: {e}");
            std::process::exit(1)
        })
    };
    let (head, prev) = (vocab("--golden-head"), vocab("--golden-prev"));
    head.difference(&prev).cloned().collect()
}

fn fail(what: &str, e: &str) -> ! {
    eprintln!("determinism_corpus: {what}: {e}");
    std::process::exit(1)
}

fn main() {
    let args: Vec<String> = std::env::args().skip(1).collect();
    match args.first().map(String::as_str) {
        Some("emit") => {
            let path = args.get(1).expect("emit PATH");
            let runtime = if args.iter().any(|a| a == "--post-latch") {
                corpus::Runtime::PostLatch
            } else {
                corpus::Runtime::PreLatch
            };
            let (entries, labels) = corpus::build_labeled_with(random(&args), runtime);
            std::fs::write(path, corpus::to_jsonl(&entries, &labels)).expect("write corpus");
            eprintln!(
                "determinism_corpus: {} entries, {} command kinds",
                entries.len(),
                corpus::covered_kinds(&entries).len()
            );
        }
        Some("command-shapes") => {
            // The token list `tests/fixtures/golden/command_shapes.v<N>.json`
            // pins (printed for inspection; the golden test writes it).
            let entries = corpus::build(0);
            let commands: Vec<Json> = entries
                .iter()
                .map(|(_, c)| serde_json::to_value(c).expect("command"))
                .collect();
            println!(
                "{}",
                serde_json::to_string_pretty(&compare::shape_list(&commands)).expect("json")
            );
        }
        Some("emit-compatible") => {
            // Regenerate the corpus without the entries a previous release's
            // probe run marked undecodable, harvesting keys from the filtered
            // engine (never dropping lines from the emitted file, which would
            // leave later commands pointing at keys that no longer exist).
            // Every drop must be explained by a golden-recorded addition
            // (#1698); the count is reported and the kept share floored.
            let probe = read(args.get(1).expect("PROBE"));
            let path = args.get(2).expect("OUT");
            let added = golden_added(&args);
            let explained = compare::explain_drops(&probe, &added)
                .unwrap_or_else(|e| fail("UNEXPLAINED DROPS", &e));
            // A lossily-decoded entry whose ignored fields all hold head's
            // default behaves identically on the old build: keep it.
            let (full, _) = corpus::build_labeled(random(&args));
            let errors: std::collections::BTreeMap<u64, &str> = probe
                .iter()
                .filter_map(|r| Some((r.get("i")?.as_u64()?, r.get("undecodable")?.as_str()?)))
                .collect();
            let mut skip = std::collections::BTreeSet::new();
            let mut benign = std::collections::BTreeSet::new();
            for (i, why) in &explained {
                let tokens = errors.get(i).and_then(|e| compare::lossy_tokens(e));
                let command = full
                    .get(*i as usize)
                    .map(|(_, c)| serde_json::to_value(c).expect("command"));
                if let (Some(tokens), Some(command)) = (tokens, command) {
                    if compare::lossy_is_benign(&command, &tokens) {
                        eprintln!(
                            "determinism_corpus: kept entry {i}: ignored {why} holds the default"
                        );
                        benign.insert(*i);
                        continue;
                    }
                }
                eprintln!("determinism_corpus: dropped entry {i}: golden-recorded additive {why}");
                skip.insert(*i);
            }
            let undecodable = compare::undecodable_indices(&probe);
            if skip.len() + benign.len() != undecodable.len() {
                fail(
                    "DROP ACCOUNTING",
                    &format!(
                        "{} undecodable entries but {} explained drops + {} benign",
                        undecodable.len(),
                        skip.len(),
                        benign.len()
                    ),
                );
            }
            let total = probe.iter().filter(|r| r.get("state").is_none()).count();
            match corpus::build_excluding(random(&args), skip.clone()) {
                Ok((entries, labels, excluded)) => {
                    std::fs::write(path, corpus::to_jsonl(&entries, &labels))
                        .expect("write corpus");
                    eprintln!(
                        "determinism_corpus: dropped {} of {total} entries ({} undecodable, all \
                         explained, + {} dependents {excluded:?}; {} benign lossy kept); {} kept \
                         ({:.1}%, floor {}%)",
                        excluded.len(),
                        skip.len(),
                        excluded.len() - skip.len(),
                        benign.len(),
                        entries.len(),
                        100.0 * entries.len() as f64 / total.max(1) as f64,
                        corpus::MIN_KEPT_PERCENT,
                    );
                }
                Err(e) => fail("EXCLUSIONS DEGRADE THE CORPUS", &e),
            }
        }
        Some("cross-release") => {
            let head = read(args.get(1).expect("HEAD"));
            let prev = read(args.get(2).expect("PREV"));
            let scenarios: Vec<String> = read(required(&args, "--corpus"))
                .iter()
                .map(|l| {
                    l.get("scenario")
                        .and_then(Json::as_str)
                        .unwrap_or_default()
                        .to_owned()
                })
                .collect();
            let allowlist = compare::parse_allowlist(
                &std::fs::read_to_string(required(&args, "--allowlist")).expect("allowlist"),
            )
            .unwrap_or_else(|e| fail("ALLOWLIST", &e));
            let added = golden_added(&args);
            let ctx = compare::CrossReleaseContext {
                scenarios: &scenarios,
                allowlist: &allowlist,
                prev_tag: required(&args, "--prev-tag"),
                added: &added,
            };
            for b in allowlist.upgrade_boundaries() {
                eprintln!(
                    "determinism_corpus: UPGRADE BOUNDARY (no rolling upgrade across this \
                     release; call out in the release notes): {b}"
                );
            }
            match compare::cross_release_checked(&head, &prev, &ctx) {
                Ok(r) => eprintln!(
                    "determinism_corpus: {} entries agree with the previous release \
                     ({} ratified-divergent, {} observational events filtered, additive fields \
                     erased {:?}); {} state checkpoints identical, {} skipped (snapshot format bump), {} compared outside the ratified \
                     upgrade-boundary instances",
                    r.base.entries,
                    r.ratified_divergent_entries,
                    r.observational_filtered,
                    r.erased_fields,
                    r.base.states_compared,
                    r.base.states_skipped_format_bump,
                    r.base.states_compared_outside_ratified_boundary
                ),
                Err(e) => fail("DIVERGENCE vs previous release", &e),
            }
        }
        _ => {
            eprintln!(
                "usage: determinism_corpus emit PATH [--random N] [--post-latch] | command-shapes | \
                 emit-compatible PROBE OUT --golden-head DIR --golden-prev DIR [--random N] | \
                 cross-release HEAD PREV --corpus COMPAT --allowlist FILE --prev-tag TAG \
                 --golden-head DIR --golden-prev DIR"
            );
            std::process::exit(2);
        }
    }
}

fn read(path: &str) -> Vec<Json> {
    compare::parse_records(&std::fs::read_to_string(path).expect("records")).expect("records")
}
