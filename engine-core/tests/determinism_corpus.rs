//! Determinism CI self-tests (ADR 0066 slice 6, #1665).
//!
//! The CI job (`scripts/determinism-ci.sh`, the `determinism` workflow job)
//! compares runner output across hash seeds, restarts, profiles, platforms and
//! the previous release. These in-process tests guard the gate itself:
//!
//! * the corpus covers **every** `Command` variant (parsed from the
//!   compiler-exhaustive `Command::kind` match — no hand-kept list to drift);
//! * a run is reproducible and a snapshot restart mid-corpus is invisible;
//! * the red half: a seeded `HashMap`-iteration non-determinism (`--canary`)
//!   reaches the records and is detected.

#![cfg(feature = "serde")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples)"
)]

#[path = "../examples/determinism/corpus.rs"]
#[allow(dead_code)]
mod corpus;
#[path = "../examples/determinism/runner.rs"]
#[allow(dead_code)]
mod runner;

use std::collections::BTreeSet;

use nanobpmn_engine_core::Engine;

const RANDOM: usize = 64;

fn corpus_jsonl() -> String {
    corpus::to_jsonl(&corpus::build(RANDOM))
}

fn run(jsonl: &str, opts: &runner::RunOptions, engine: &mut Engine) -> String {
    let entries = runner::read_corpus(jsonl.as_bytes()).unwrap();
    let mut out = Vec::new();
    runner::run(engine, entries, opts, &mut out).unwrap();
    String::from_utf8(out).unwrap()
}

fn full_run(jsonl: &str, canary: bool) -> String {
    let opts = runner::RunOptions {
        state_every: 16,
        canary,
        ..Default::default()
    };
    run(jsonl, &opts, &mut Engine::new())
}

/// Every `"kind"` string in `Command::kind`'s exhaustive match.
fn all_command_kinds() -> BTreeSet<String> {
    let src = include_str!("../src/command.rs");
    let body = src
        .split("pub fn kind(&self) -> &'static str {")
        .nth(1)
        .expect("Command::kind present")
        .split("\n    }\n")
        .next()
        .unwrap();
    body.lines()
        .filter(|l| l.trim_start().starts_with("Command::"))
        .filter_map(|l| l.split("=> \"").nth(1))
        .map(|s| s.trim_end_matches("\",").to_string())
        .collect()
}

#[test]
fn corpus_covers_every_command_variant() {
    let all = all_command_kinds();
    assert!(all.len() > 40, "kind parser broke: {all:?}");
    let covered: BTreeSet<String> = corpus::covered_kinds(&corpus::build(RANDOM))
        .into_iter()
        .map(str::to_string)
        .collect();
    let missing: Vec<_> = all.difference(&covered).collect();
    assert!(
        missing.is_empty(),
        "corpus misses Command variants: {missing:?}"
    );
}

#[test]
fn corpus_generation_is_deterministic() {
    assert_eq!(corpus_jsonl(), corpus_jsonl());
}

#[test]
fn corpus_exercises_the_engine_not_just_rejections() {
    let records = full_run(&corpus_jsonl(), false);
    let ok = records.lines().filter(|l| l.contains("\"events\"")).count();
    let err = records.lines().filter(|l| l.contains("\"err\"")).count();
    assert!(
        ok > err * 3,
        "corpus is mostly rejections: ok={ok} err={err}"
    );
    assert!(!records.contains("\"undecodable\""));
}

#[test]
fn replay_is_reproducible_and_restart_invisible() {
    let jsonl = corpus_jsonl();
    let full = full_run(&jsonl, false);
    assert_eq!(full, full_run(&jsonl, false));

    let total = jsonl.lines().count() as u64;
    for split in [1, total / 3, total / 2 + 1] {
        let mut first_engine = Engine::new();
        let first = run(
            &jsonl,
            &runner::RunOptions {
                state_every: 16,
                stop_after: Some(split),
                ..Default::default()
            },
            &mut first_engine,
        );
        // A "process restart": serialize, restore into a fresh engine.
        let snap = runner::save_snapshot(&first_engine).unwrap();
        let mut resumed = runner::load_snapshot(&snap).unwrap();
        let second = run(
            &jsonl,
            &runner::RunOptions {
                state_every: 16,
                start_at: split,
                ..Default::default()
            },
            &mut resumed,
        );
        assert_eq!(
            full,
            format!("{first}{second}"),
            "restart at {split} diverged"
        );
    }
}

/// Red test (#1665): a deliberate `HashMap`-iteration non-determinism that
/// reaches the events must be detected. Each `HashMap::new()` draws fresh
/// `RandomState` keys, so two runs permute the corpus's distinct
/// `CreateInstance` entries differently — with well over 32 of them, the
/// chance of an identical order is negligible (< 1/32!).
#[test]
fn seeded_hashmap_nondeterminism_is_detected() {
    let jsonl = corpus_jsonl();
    let creates = jsonl
        .lines()
        .filter(|l| l.contains("\"CreateInstance\""))
        .count();
    assert!(
        creates > 32,
        "canary needs >32 distinct creates, has {creates}"
    );
    let a = full_run(&jsonl, true);
    let b = full_run(&jsonl, true);
    assert_ne!(a, b, "canary non-determinism escaped the comparison");
}
