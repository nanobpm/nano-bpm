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

#[path = "../examples/determinism/compare.rs"]
#[allow(dead_code)]
mod compare;
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

/// Every full run ends with a state checkpoint at the last entry, so the
/// trailing partial interval (the newest config commands) is state-compared;
/// a `--stop-after` split adds none (restart test above stays byte-equal).
#[test]
fn full_run_checkpoints_final_state() {
    let jsonl = corpus_jsonl();
    let last = jsonl.lines().count() as u64 - 1;
    let records = full_run(&jsonl, false);
    let tail: serde_json::Value = serde_json::from_str(records.lines().last().unwrap()).unwrap();
    assert_eq!(tail.get("i").and_then(|v| v.as_u64()), Some(last));
    assert!(tail.get("state").is_some() && tail.get("format").is_some());
}

fn records(text: &str) -> Vec<serde_json::Value> {
    compare::parse_records(text).unwrap()
}

#[test]
fn cross_release_compares_durable_state() {
    let jsonl = corpus_jsonl();
    let head = records(&full_run(&jsonl, false));
    let report = compare::cross_release(&head, &head).unwrap();
    assert!(report.states_compared > 0);
    assert_eq!(report.states_skipped_format_bump, 0);

    // Identical events, divergent durable state at the same format: caught.
    let mut prev = head.clone();
    let state = prev
        .iter_mut()
        .rev()
        .find(|r| r.get("state").is_some())
        .unwrap();
    state["state"]["next_local"] = serde_json::json!(987_654_321u64);
    let err = compare::cross_release(&head, &prev).unwrap_err();
    assert!(err.contains("durable state diverges"), "{err}");

    // A snapshot-format bump is the explicit shape-change decision: skipped.
    for r in prev.iter_mut().filter(|r| r.get("state").is_some()) {
        r["format"] = serde_json::json!(0);
    }
    let report = compare::cross_release(&head, &prev).unwrap();
    assert_eq!(report.states_compared, 0);
    assert!(report.states_skipped_format_bump > 0);

    // Missing checkpoints are not a silent pass.
    let no_state: Vec<_> = head
        .iter()
        .filter(|r| r.get("state").is_none())
        .cloned()
        .collect();
    assert!(compare::cross_release(&no_state, &no_state).is_err());
}

/// Excluding entries a previous release cannot decode regenerates the corpus
/// with re-harvested keys; kept entries that depend on an excluded one are
/// excluded too (never silently compared as degraded executions), and losing
/// a core dependency is a hard failure.
#[test]
fn exclusions_regenerate_keys_and_must_not_degrade_kept_entries() {
    let full = corpus::build(RANDOM);
    let last = full.len() as u64 - 1;

    // A self-contained tail entry: exactly one fewer entry, rest intact.
    let (tail, excluded) = corpus::build_excluding(RANDOM, [last].into()).unwrap();
    assert_eq!(tail.len(), full.len() - 1);
    assert_eq!(excluded, [last].into());

    // A key-allocating, self-contained entry (DeployForms, position 5)
    // shifts every later key; re-harvesting keeps every kept outcome.
    let (_, excluded) = corpus::build_excluding(RANDOM, [5].into()).unwrap();
    assert_eq!(excluded, [5].into());

    // A newer model shape (the event sub-process deployment, position 3):
    // its dependents go with it, the core corpus survives.
    let (_, excluded) = corpus::build_excluding(RANDOM, [3].into()).unwrap();
    assert!(excluded.len() > 1, "dependents excluded: {excluded:?}");

    // The core order-process deployment: most of the corpus depends on it.
    assert!(corpus::build_excluding(RANDOM, [0].into()).is_err());
}
