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

use nanobpmn_engine_core::{Command, Engine};

const RANDOM: usize = 64;

fn corpus_jsonl() -> String {
    let (entries, labels) = corpus::build_labeled(RANDOM);
    corpus::to_jsonl(&entries, &labels)
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

/// A corpus entry that passes variant coverage while emitting NOTHING leaves
/// the path it names untested — the defect class behind the review findings
/// that each of these scenarios was a silent no-op. Assert each scenario's
/// expected events actually appear in the records, so the gate exercises the
/// path rather than only its rejection.
#[test]
fn corpus_scenarios_emit_their_events() {
    let records_text = full_run(&corpus_jsonl(), false);
    let parsed = records(&records_text);
    let records = records_text.as_str();

    // Transcendental floats (ADR 0066 audit #7): the script results persist
    // into variables, so platform-dependent bits would reach the records.
    // Inspect `VariablesUpdated` events only — the deployed script text also
    // contains `ln:`/`exp:`/`pow:`, so a whole-log substring search would pass
    // on deployment alone. Require a persisted result for BOTH instances.
    let persisted: BTreeSet<u64> = parsed
        .iter()
        .filter_map(|r| r.get("events")?.as_array().cloned())
        .flatten()
        .filter_map(|e| {
            let u = e.get("VariablesUpdated")?;
            let s = u
                .get("variables")?
                .get("transcendental")?
                .get("Str")?
                .as_str()?;
            let ok = ["ln:", ",exp:", ",pow:"].iter().all(|p| s.contains(p)) && !s.contains("null");
            ok.then(|| u.get("instance_key")?.as_u64()).flatten()
        })
        .collect();
    assert_eq!(
        persisted.len(),
        2,
        "transcendental FEEL results (log/exp/**) must persist via VariablesUpdated \
         for both `floats` instances, got {persisted:?}"
    );

    // Selective expiry: both durabilities must release locks — no expiry
    // entry may be an empty no-op behind the unfiltered sweep.
    let expired = records.matches("JobLockExpired").count();
    assert!(
        expired >= 4,
        "selective + unfiltered expiry released {expired} locks, expected >= 4 \
         (2 durable by key + soft activations + unfiltered sweep)"
    );

    // Routed subscription protocol: the generator's `routed_protocol_entries`
    // asserts at BUILD time that a real two-partition pair opens, correlates
    // and advances a harvested `Opening` subscription (a no-op there panics
    // the corpus build). The routed commands are recorded as corpus entries;
    // in the runner they replay against the single engine, which holds the
    // subscription records by their (partition-0-local) keys, so the open and
    // close emit their events and the continuation is deduped by its
    // `delivery_key` (the at-least-once path, #1497).
    for variant in [
        "MessageSubscriptionCreated",
        "RoutedDeliveryRecorded",
        "MessageSubscriptionCanceled",
    ] {
        assert!(
            records.contains(variant),
            "routed subscription protocol never emitted {variant}"
        );
    }

    // Orphan quarantine, both forms against live instances: the local form's
    // `VariablesLost` incident and the replicated form's dedup tombstone.
    assert!(
        records.matches("VariablesLost").count() >= 2,
        "quarantine incidents missing (local + replicated + cold forms)"
    );
    assert!(
        records.contains("VariablesLostQuarantined"),
        "the replicated (stamped) quarantine form never emitted its tombstone"
    );
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

fn labels() -> Vec<String> {
    corpus::build_labeled(RANDOM)
        .1
        .into_iter()
        .map(str::to_owned)
        .collect()
}

/// `cross_release_checked` over `head`/`prev` with `allowlist` (JSON) and
/// `added` as the golden-derived additive set, previous release `v0.0.1`.
fn checked(
    head: &[serde_json::Value],
    prev: &[serde_json::Value],
    allowlist: serde_json::Value,
    added: &[&str],
) -> Result<compare::CheckedReport, String> {
    let scenarios = labels();
    let allowlist = compare::parse_allowlist(&allowlist.to_string())?;
    let added: compare::Vocab = added.iter().map(|t| (*t).to_owned()).collect();
    let ctx = compare::CrossReleaseContext {
        scenarios: &scenarios,
        allowlist: &allowlist,
        prev_tag: "v0.0.1",
        added: &added,
    };
    compare::cross_release_checked(head, prev, &ctx)
}

fn no_allowlist() -> serde_json::Value {
    serde_json::json!({})
}

fn head_records() -> Vec<serde_json::Value> {
    records(&full_run(&corpus_jsonl(), false))
}

/// The entry records (not state checkpoints) of `records`, mutably.
fn entries_mut(records: &mut [serde_json::Value]) -> Vec<&mut serde_json::Value> {
    records
        .iter_mut()
        .filter(|r| r.get("state").is_none())
        .collect()
}

/// Removes every `variant` event from `records` (a release that never emits
/// it).
fn without_variant(records: &[serde_json::Value], variant: &str) -> Vec<serde_json::Value> {
    let mut out = records.to_vec();
    for r in entries_mut(&mut out) {
        if let Some(serde_json::Value::Array(events)) = r.get_mut("events") {
            events.retain(|e| e.get(variant).is_none());
        }
    }
    out
}

/// The first entry index of `scenario` whose events contain at least two
/// distinct events.
fn reorderable_entry(records: &[serde_json::Value], scenario: &str) -> usize {
    let scenarios = labels();
    records
        .iter()
        .filter(|r| r.get("state").is_none())
        .enumerate()
        .find(|(i, r)| {
            scenarios[*i] == scenario
                && r.get("events")
                    .and_then(|e| e.as_array())
                    .is_some_and(|e| e.len() >= 2 && e.first() != e.last())
        })
        .map(|(i, _)| i)
        .expect("a multi-event entry in the scenario")
}

/// The first entry index of `scenario` whose events emit `variant`.
fn entry_emitting(records: &[serde_json::Value], scenario: &str, variant: &str) -> usize {
    let scenarios = labels();
    records
        .iter()
        .filter(|r| r.get("state").is_none())
        .enumerate()
        .find(|(i, r)| {
            scenarios[*i] == scenario
                && r.get("events")
                    .and_then(|e| e.as_array())
                    .is_some_and(|evs| evs.iter().any(|e| e.get(variant).is_some()))
        })
        .map(|(i, _)| i)
        .unwrap_or_else(|| panic!("an entry emitting {variant} in scenario {scenario}"))
}

#[test]
fn cross_release_compares_durable_state() {
    let head = head_records();
    let report = checked(&head, &head, no_allowlist(), &[]).unwrap();
    assert!(report.base.states_compared > 0);
    assert_eq!(report.base.states_skipped_format_bump, 0);

    // Identical events, divergent durable state at the same format: caught.
    let mut prev = head.clone();
    let state = prev
        .iter_mut()
        .rev()
        .find(|r| r.get("state").is_some())
        .unwrap();
    state["state"]["next_local"] = serde_json::json!(987_654_321u64);
    let err = checked(&head, &prev, no_allowlist(), &[]).unwrap_err();
    assert!(err.contains("durable state diverges"), "{err}");

    // A snapshot-format bump is the explicit shape-change decision: skipped.
    for r in prev.iter_mut().filter(|r| r.get("state").is_some()) {
        r["format"] = serde_json::json!(0);
    }
    let report = checked(&head, &prev, no_allowlist(), &[]).unwrap();
    assert_eq!(report.base.states_compared, 0);
    assert!(report.base.states_skipped_format_bump > 0);

    // Missing checkpoints are not a silent pass.
    let no_state: Vec<_> = head
        .iter()
        .filter(|r| r.get("state").is_none())
        .cloned()
        .collect();
    assert!(checked(&no_state, &no_state, no_allowlist(), &[]).is_err());
}

/// #1698: a new event variant the previous release never emits fails unless
/// it is ratified as observational — and then it passes, filtered.
#[test]
fn prev_release_gate_undeclared_new_variant_fails_declared_observational_passes() {
    let head = head_records();
    let prev = without_variant(&head, "JobCanceled");
    let err = checked(&head, &prev, no_allowlist(), &["JobCanceled"]).unwrap_err();
    assert!(
        err.contains("UNDECLARED new event variant `JobCanceled`"),
        "{err}"
    );

    let allow = serde_json::json!({"observational_variants": [{
        "variant": "JobCanceled", "introduced_in": "v0.0.2", "issue": "#0",
        "upgrade": "upgrade-boundary", "rationale": "upgrade-boundary: test"
    }]});
    let report = checked(&head, &prev, allow.clone(), &["JobCanceled"]).unwrap();
    assert!(report.observational_filtered > 0);

    // Only a golden-recorded additive variant can be ratified.
    let err = checked(&head, &prev, allow, &[]).unwrap_err();
    assert!(err.contains("not recorded by the golden"), "{err}");
}

/// #1698: a golden-added field the previous release never emits is erased; a
/// field the golden does not record as added is a real divergence.
#[test]
fn prev_release_gate_erases_only_golden_added_fields() {
    let head = head_records();
    let mut prev = head.clone();
    for r in entries_mut(&mut prev) {
        if let Some(serde_json::Value::Array(events)) = r.get_mut("events") {
            for e in events {
                if let Some(body) = e.get_mut("JobCanceled").and_then(|b| b.as_object_mut()) {
                    body.remove("canceled_at");
                }
            }
        }
    }
    // (Re-decoded through the head type, the missing field defaults to 0.)
    let err = checked(&head, &prev, no_allowlist(), &[]).unwrap_err();
    assert!(err.contains("canceled_at"), "{err}");
    let report = checked(&head, &prev, no_allowlist(), &["JobCanceled.canceled_at"]).unwrap();
    assert_eq!(report.erased_fields, ["JobCanceled.canceled_at".to_owned()]);
}

/// #1698: an allowlisted scenario admits exactly its declared divergence;
/// anything more fails, and an entry the run does not need is stale.
#[test]
fn prev_release_gate_allowlisted_scenario_admits_only_the_declared_divergence() {
    let head = head_records();
    let scenario = "jobs-and-user-tasks";
    let i = reorderable_entry(&head, scenario);
    let declare = |expected: serde_json::Value, introduced: &str| {
        serde_json::json!({"divergences": [{
            "scenario": scenario, "expected": expected, "introduced_in": introduced,
            "issue": "#0", "upgrade": "upgrade-boundary", "rationale": "upgrade-boundary: test"
        }]})
    };
    let reorder = serde_json::json!({"reorder": true});

    // Reordered events: undeclared → fails; declared `reorder` → passes.
    let mut prev = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[i].get_mut("events") {
        events.reverse();
    }
    let err = checked(&head, &prev, no_allowlist(), &[]).unwrap_err();
    assert!(err.contains(&format!("scenario {scenario}")), "{err}");
    let report = checked(&head, &prev, declare(reorder.clone(), "v0.0.2"), &[]).unwrap();
    assert_eq!(report.ratified_divergent_entries, 1);

    // An undeclared EXTRA divergence in the allowlisted scenario fails.
    let mut extra = prev.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut extra)[i].get_mut("events") {
        events.pop();
    }
    let err = checked(&head, &extra, declare(reorder.clone(), "v0.0.2"), &[]).unwrap_err();
    assert!(err.contains("UNDECLARED extra divergence"), "{err}");

    // Stale: the scenario no longer diverges.
    let err = checked(&head, &head, declare(reorder.clone(), "v0.0.2"), &[]).unwrap_err();
    assert!(
        err.contains("STALE") && err.contains("no longer diverges"),
        "{err}"
    );

    // Expired: the previous release (v0.0.1) is at or past the introducing
    // release, so both builds run the new behaviour.
    let err = checked(&head, &prev, declare(reorder, "v0.0.1"), &[]).unwrap_err();
    assert!(err.contains("STALE") && err.contains("at or past"), "{err}");
}

/// #1535 / #1632: a behavioural change that REPLACES one event with another
/// (termination-for-completion) or newly populates a golden-added field
/// (adhoc `ElementActivating.scope`) is ratified by `variant_substitutions` /
/// `field_overrides`, and the new variant is not rejected as undeclared when
/// the scenario declares it in `only_in_head`.
#[test]
fn prev_release_gate_variant_substitution_and_field_override() {
    let head = head_records();
    let scenario = "jobs-and-user-tasks";
    // An entry whose events include BOTH an `ElementCompleted` (the
    // substitution source) and an `ElementActivated` (the field-override
    // target), so one entry exercises both mechanisms.
    let i = {
        let scenarios = labels();
        head.iter()
            .filter(|r| r.get("state").is_none())
            .enumerate()
            .find(|(idx, r)| {
                scenarios[*idx] == scenario
                    && r.get("events")
                        .and_then(|e| e.as_array())
                        .is_some_and(|ev| {
                            ev.iter().any(|e| e.get("ElementCompleted").is_some())
                                && ev.iter().any(|e| e.get("ElementActivated").is_some())
                        })
            })
            .map(|(idx, _)| idx)
            .expect("an entry with both ElementCompleted and ElementActivated")
    };

    // --- variant_substitutions: prev completes what head terminates. Build a
    // prev that renames head's ElementCompleted -> ElementTerminated on the
    // entry, so head has ElementCompleted only and prev has ElementTerminated
    // only for the same entity.
    let mut prev = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[i].get_mut("events") {
        for e in events.iter_mut() {
            let obj = e.as_object_mut().unwrap();
            if let Some(body) = obj.remove("ElementCompleted") {
                obj.insert("ElementTerminated".to_owned(), body);
            }
        }
    }
    // Undeclared: the substitution is a divergence (the new variant is not
    // golden-added here, so it surfaces as a plain per-entry divergence).
    let err = checked(&head, &prev, no_allowlist(), &[]).unwrap_err();
    assert!(err.contains(&format!("scenario {scenario}")), "{err}");
    // Declared via only_in_head/only_in_prev + variant_substitutions: passes.
    let allow = serde_json::json!({"divergences": [{
        "scenario": scenario,
        "expected": {
            "only_in_prev": [{"variant": "ElementTerminated"}],
            "only_in_head": [{"variant": "ElementCompleted"}],
            "variant_substitutions": [{"prev": "ElementTerminated", "head": "ElementCompleted"}]
        },
        "introduced_in": "v0.0.2", "issue": "#0", "upgrade": "upgrade-boundary",
        "rationale": "upgrade-boundary: test"
    }]});
    let report = checked(&head, &prev, allow, &["ElementTerminated"]).unwrap();
    assert_eq!(report.ratified_divergent_entries, 1);

    // --- field_overrides: head populates a golden-added field prev decodes
    // to the default. Give head's ElementActivated a non-zero scope where prev
    // has none (decodes to 0).
    let mut head2 = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut head2)[i].get_mut("events") {
        for e in events.iter_mut() {
            if let Some(body) = e.get_mut("ElementActivated") {
                body["scope"] = serde_json::json!(123u64);
            }
        }
    }
    // Undeclared: the populated field is a real divergence.
    let err = checked(&head2, &head, no_allowlist(), &["ElementActivated.scope"]).unwrap_err();
    assert!(err.contains(&format!("scenario {scenario}")), "{err}");
    // Declared via field_overrides: passes.
    let allow = serde_json::json!({"divergences": [{
        "scenario": scenario,
        "expected": {"field_overrides": [{"variant": "ElementActivated", "field": "scope", "value": 0}]},
        "introduced_in": "v0.0.2", "issue": "#0", "upgrade": "upgrade-boundary",
        "rationale": "upgrade-boundary: test"
    }]});
    let report = checked(&head2, &head, allow, &["ElementActivated.scope"]).unwrap();
    assert_eq!(report.ratified_divergent_entries, 1);
}

/// #1698 (review): `remint_keys` rewrites every key reference to the identity
/// of the entity it references (from that side's own creation events), so a
/// clean re-mint (a bijection on keys, the #1244 fork order) reconciles while
/// a reference to the wrong or an un-minted entity still diverges — including
/// a KEY-ONLY change (the `element_id` untouched) to a completion, a job, or
/// a subscription reference, which zeroing the keys would hide. An entry
/// whose minting is not a clean one-key-per-entity (two keys for one
/// `element_id`, or one key for two) fails closed rather than reconcile.
#[test]
fn prev_release_gate_remint_keys_preserves_entity_relationships() {
    let head = head_records();
    let scenario = "jobs-and-user-tasks";
    let i = reorderable_entry(&head, scenario);
    let allow = serde_json::json!({"divergences": [{
        "scenario": scenario,
        "expected": {"reorder": true, "remint_keys": true},
        "introduced_in": "v0.0.2", "issue": "#0", "upgrade": "upgrade-boundary", "rationale": "upgrade-boundary: test"
    }]});

    // A pure reorder (same keys, different order): declared, passes.
    let mut prev = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[i].get_mut("events") {
        events.reverse();
    }
    let report = checked(&head, &prev, allow.clone(), &[]).unwrap();
    assert_eq!(report.ratified_divergent_entries, 1);

    // A completion event naming a DIFFERENT element_id than its key's creation
    // event: the (element_id, key) pair no longer matches, so it diverges.
    let mut broken = prev.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut broken)[i].get_mut("events") {
        let mut completion = events
            .iter_mut()
            .find(|e| e.get("ElementCompleted").is_some());
        if let Some(c) = completion.as_deref_mut() {
            c["ElementCompleted"]["element_id"] = serde_json::json!("no-such-element");
        }
        assert!(completion.is_some());
    }
    let err = checked(&head, &broken, allow.clone(), &[]).unwrap_err();
    assert!(err.contains("UNDECLARED extra divergence"), "{err}");

    // KEY-ONLY reference changes (the element_id untouched): point a
    // completion, a job, and a subscription reference at a key the entry never
    // minted. Zeroing the keys would hide all three; canonicalizing each key
    // to its referenced entity turns the un-minted key into a distinct orphan
    // token, so each still diverges. Each case mutates one event in an entry
    // of an allowlisted `remint_keys` scenario and asserts the gate fails.
    //
    // `key_only` returns a `prev` that is `head` with one `variant.field`
    // (a u64 key) in the `pos`-th entry of `scenario` set to an un-minted key.
    let key_only = |scenario: &str, pos: usize, variant: &str, field: &str| {
        let mut p = head.clone();
        let scenarios = labels();
        let mut touched = false;
        for (entry_idx, r) in entries_mut(&mut p).into_iter().enumerate() {
            if scenarios[entry_idx] == scenario && entry_idx == pos {
                if let Some(serde_json::Value::Array(events)) = r.get_mut("events") {
                    for e in events.iter_mut() {
                        if let Some(body) = e.get_mut(variant) {
                            if body.get(field).and_then(|x| x.as_u64()).is_some() {
                                body[field] = serde_json::json!(9_999_999u64); // never minted
                                touched = true;
                                break;
                            }
                        }
                    }
                }
            }
            if touched {
                break;
            }
        }
        assert!(touched, "{scenario}[{pos}] must emit {variant}.{field}");
        p
    };
    // Completion + completing references on the reordered entry.
    for field_variant in ["ElementCompleted", "ElementCompleting"] {
        let keyonly = key_only(scenario, i, field_variant, "element_instance_key");
        let err = checked(&head, &keyonly, allow.clone(), &[]).unwrap_err();
        assert!(
            err.contains("UNDECLARED extra divergence"),
            "key-only {field_variant}.element_instance_key change must diverge: {err}"
        );
    }
    // A job reference on a job-COMPLETING entry of the same scenario:
    // `JobCompleted.job_key` refers to the key `JobCreated` minted, so
    // pointing it at an un-minted key is an inconsistent reference.
    let job_pos = entry_emitting(&head, scenario, "JobCompleted");
    let keyonly = key_only(scenario, job_pos, "JobCompleted", "job_key");
    let err = checked(&head, &keyonly, allow.clone(), &[]).unwrap_err();
    assert!(
        err.contains("UNDECLARED extra divergence"),
        "key-only JobCompleted.job_key change must diverge: {err}"
    );
    // A subscription reference on the messages scenario (allowlisted below):
    // `MessageCorrelated.subscription_key` refers to the key the subscription
    // was created under, so pointing it at an un-minted key is inconsistent.
    let sub_scenario = "messages-signals-conditional";
    let sub_pos = entry_emitting(&head, sub_scenario, "MessageCorrelated");
    let sub_allow = serde_json::json!({"divergences": [{
        "scenario": sub_scenario,
        "expected": {"reorder": true, "remint_keys": true},
        "introduced_in": "v0.0.2", "issue": "#0", "upgrade": "upgrade-boundary", "rationale": "upgrade-boundary: test"
    }]});
    let keyonly = key_only(
        sub_scenario,
        sub_pos,
        "MessageCorrelated",
        "subscription_key",
    );
    let err = checked(&head, &keyonly, sub_allow, &[]).unwrap_err();
    assert!(
        err.contains("UNDECLARED extra divergence"),
        "key-only MessageCorrelated.subscription_key change must diverge: {err}"
    );

    // An entry whose creation events mint TWO distinct keys for one
    // element_id is not a clean remint: fails closed.
    let mut colliding = prev.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut colliding)[i].get_mut("events")
    {
        let mut seen: std::collections::BTreeMap<String, u64> = Default::default();
        for e in events.iter_mut() {
            for variant in ["ElementActivating", "ElementActivated"] {
                if let Some(body) = e.get_mut(variant) {
                    let id = body
                        .get("element_id")
                        .and_then(|x| x.as_str())
                        .unwrap_or("");
                    let key = body
                        .get("element_instance_key")
                        .and_then(|x| x.as_u64())
                        .unwrap_or(0);
                    if let Some(prev_key) = seen.get(id) {
                        // Mint a fresh, distinct key for the same element.
                        body["element_instance_key"] = serde_json::json!(prev_key + 10_000);
                    } else {
                        seen.insert(id.to_owned(), key);
                    }
                }
            }
        }
    }
    let err = checked(&head, &colliding, allow, &[]).unwrap_err();
    assert!(err.contains("UNDECLARED extra divergence"), "{err}");
}

/// #1698 (review): a snake_case key inside a map-typed field (a variable
/// name like `variables.receipt`) is map DATA, not a struct field — the
/// golden-added erasure must never remove it, so a variable the previous
/// stream omits stays a divergence even when the golden corpus records a
/// same-named field as added. The variable must be absent from the ENTIRE
/// previous stream: a `BTreeMap<String, Value>` is a data boundary
/// (`is_value_map`), so its keys are never treated as struct fields in
/// vocabulary extraction or erasure.
#[test]
fn prev_release_gate_never_erases_map_data() {
    let head = head_records();
    // A snake_case variable name absent from the ENTIRE corpus (so the
    // previous stream never emits it anywhere).
    let var = "zz_novel_receipt";
    let prev = head.clone(); // previous stream: never emits `var`
    let mut head_with_var = head.clone();
    let mut touched = false;
    for h in entries_mut(&mut head_with_var) {
        if let Some(serde_json::Value::Array(events)) = h.get_mut("events") {
            for e in events {
                if let Some(vars) = e
                    .get_mut("VariablesUpdated")
                    .and_then(|b| b.get_mut("variables"))
                    .and_then(|v| v.as_object_mut())
                {
                    vars.insert(var.to_owned(), serde_json::json!({"Bool": true}));
                    touched = true;
                }
            }
        }
        if touched {
            break;
        }
    }
    assert!(touched, "the corpus must emit a VariablesUpdated event");
    // The golden records `VariablesUpdated.zz_novel_receipt` as added since
    // the previous release. If erasure wrongly treated the variable map's
    // snake_case key as that struct field, the missing variable would be
    // normalized away and the gate would pass — it must FAIL.
    let err = checked(
        &head_with_var,
        &prev,
        no_allowlist(),
        &["VariablesUpdated.zz_novel_receipt"],
    )
    .unwrap_err();
    assert!(err.contains("scenario"), "{err}");
}

/// #1698: every entry the previous release cannot decode must be explained
/// by the golden-derived additive set; an unexplained drop fails.
#[test]
fn prev_release_gate_unexplained_drop_fails() {
    let probe = |err: &str| vec![serde_json::json!({"i": 3, "undecodable": err})];
    let added: compare::Vocab = [
        "DeployBundle".to_owned(),
        "ModifyInstance.terminate_element_ids".to_owned(),
    ]
    .into();
    let variant = probe("unknown variant `DeployBundle`, expected one of ...");
    assert_eq!(compare::explain_drops(&variant, &added).unwrap().len(), 1);
    let lossy = probe("lossy decode: drops field `ModifyInstance.terminate_element_ids`");
    assert_eq!(compare::explain_drops(&lossy, &added).unwrap().len(), 1);

    for unexplained in [
        "unknown variant `SomethingElse`, expected one of ...",
        "lossy decode: drops field `ModifyInstance.other`",
        "invalid type: string, expected u64",
    ] {
        let err = compare::explain_drops(&probe(unexplained), &added).unwrap_err();
        assert!(err.contains("NOT explained"), "{err}");
    }
}

/// #1698: a field the previous release silently ignores is reported as a
/// lossy decode (not applied as a different command), unless it holds the
/// default.
#[test]
fn prev_release_gate_detects_lossy_decode() {
    let modify = serde_json::json!({"ModifyInstance": {
        "instance_key": 1, "activate_instructions": [], "move_instructions": [],
        "terminate_instructions": [], "terminate_element_ids": ["pay"]
    }});
    assert!(!compare::lossy_is_benign(
        &modify,
        &["ModifyInstance.terminate_element_ids".to_owned()]
    ));
    let full = corpus::build(RANDOM);
    let deploy = serde_json::to_value(&full[0].1).unwrap();
    assert!(compare::lossy_is_benign(
        &deploy,
        &["UserTask.form_binding_type".to_owned()]
    ));

    // The runner reports an unknown, non-empty field instead of applying.
    let line = format!(
        "{}\n",
        serde_json::json!({"now": 1, "command": {"CancelInstance": {
            "instance_key": 1, "from_the_future": "x"
        }}})
    );
    let opts = runner::RunOptions {
        skip_undecodable: true,
        ..Default::default()
    };
    let out = run(&line, &opts, &mut Engine::new());
    assert!(
        out.contains("lossy decode: drops field `CancelInstance.from_the_future`"),
        "{out}"
    );

    // An EMPTY-valued unknown field is reported too: whether ignoring it is
    // benign is `lossy_is_benign`'s default-aware call, not the runner's —
    // an explicit `false` against a default-`true` field (e.g.
    // `MultiInstance.cancel_remaining_instances`) changes behaviour.
    let line = format!(
        "{}\n",
        serde_json::json!({"now": 1, "command": {"CancelInstance": {
            "instance_key": 1, "from_the_future": false
        }}})
    );
    let out = run(&line, &opts, &mut Engine::new());
    assert!(
        out.contains("lossy decode: drops field `CancelInstance.from_the_future`"),
        "{out}"
    );

    // `lossy_is_benign` does the classifying: an empty-valued field whose
    // HEAD default is non-empty is NOT benign (dropped), while one whose
    // default is the empty value is (kept). A `MultiInstance` field's golden
    // token is qualified by the enclosing COMMAND variant (`DeployProcess`),
    // and `cancel_remaining_instances` defaults to `true` (model.rs), so an
    // explicit `false` the old build ignores changes behaviour.
    let deploy_with_mi = |mi: serde_json::Value| {
        serde_json::json!({"DeployProcess": {
            "id": "p", "start_event": "s",
            "elements": {"s": {
                "id": "s",
                "kind": {"ServiceTask": {"job_type": "t"}},
                "outgoing": [],
                "multi_instance": mi
            }}
        }})
    };
    assert!(!compare::lossy_is_benign(
        &deploy_with_mi(serde_json::json!({
            "input_collection": "xs", "cancel_remaining_instances": false
        })),
        &["DeployProcess.cancel_remaining_instances".to_owned()]
    ));
    assert!(compare::lossy_is_benign(
        &deploy_with_mi(serde_json::json!({
            "input_collection": "xs", "sequential": false
        })),
        &["DeployProcess.sequential".to_owned()]
    ));
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
    let (tail, _, excluded) = corpus::build_excluding(RANDOM, [last].into()).unwrap();
    assert_eq!(tail.len(), full.len() - 1);
    assert_eq!(excluded, [last].into());

    // A key-allocating, self-contained entry (DeployForms) shifts every later
    // key; re-harvesting keeps every kept outcome.
    let deploy_forms = position_of(&full, "deploy_forms");
    assert_eq!(deploy_forms.len(), 1, "one DeployForms entry");
    let (_, _, excluded) =
        corpus::build_excluding(RANDOM, deploy_forms.iter().copied().collect()).unwrap();
    assert_eq!(excluded, deploy_forms.into_iter().collect());

    // A newer model shape (the event sub-process deployment): its dependents
    // go with it, the core corpus survives.
    let esp_deploy = position_of(&full, "deploy_process")
        .into_iter()
        .find(|&p| matches!(&full[p as usize].1, Command::DeployProcess(d) if d.id == "escalation"))
        .expect("escalation deploy");
    let (_, _, excluded) = corpus::build_excluding(RANDOM, [esp_deploy].into()).unwrap();
    assert!(excluded.len() > 1, "dependents excluded: {excluded:?}");

    // The core order-process deployment: most of the corpus depends on it.
    assert!(corpus::build_excluding(RANDOM, [0].into()).is_err());
}

/// Positions of every corpus entry whose command kind is `kind`.
fn position_of(full: &[(u64, Command)], kind: &str) -> Vec<u64> {
    full.iter()
        .enumerate()
        .filter(|(_, (_, c))| c.kind() == kind)
        .map(|(i, _)| i as u64)
        .collect()
}

/// #1700 review: a ratified remint swaps which entity a raw key names on
/// each side. A LATER entry whose raw events are byte-identical on both
/// sides but reference a swapped key names DIFFERENT entities — the
/// key-identity mapping is carried across the whole corpus, so it diverges.
#[test]
fn prev_release_gate_remint_identity_carries_across_entries() {
    let head = head_records();
    let scenarios = labels();
    let entries: Vec<&serde_json::Value> =
        head.iter().filter(|r| r.get("state").is_none()).collect();
    let activations = |r: &serde_json::Value| -> Vec<(u64, String)> {
        r.get("events")
            .and_then(|e| e.as_array())
            .into_iter()
            .flatten()
            .filter_map(|e| e.get("ElementActivating"))
            .filter_map(|b| {
                Some((
                    b.get("element_instance_key")?.as_u64()?,
                    b.get("element_id")?.as_str()?.to_owned(),
                ))
            })
            .collect()
    };
    let referenced_later = |from: usize, key: u64| {
        entries[from + 1..].iter().any(|r| {
            r.get("events")
                .and_then(|e| e.as_array())
                .into_iter()
                .flatten()
                .filter_map(|e| e.as_object())
                .flat_map(|o| o.values())
                .any(|b| b.get("element_instance_key").and_then(|k| k.as_u64()) == Some(key))
        })
    };
    // An entry in `scenario` minting two distinct elements, the first of
    // which a later entry references. The pair must be COLLISION-FREE under
    // the identity space: identities are unique per side, not across sides,
    // so swapping k1<->k2 must not make prev's base for k2
    // (`element:I2:e2`) equal an identity head mints elsewhere
    // (`element:I1:e2` with I2's element minted in an earlier entry) — that
    // would misattribute later same-element references. Require that no
    // EARLIER entry mints an element instance for `e2` in a different
    // process instance than k2's.
    let minted_earlier = |upto: usize, element: &str, instance: u64| {
        entries[..upto].iter().any(|r| {
            activations(r).iter().any(|(_, e)| e == element)
                && r.get("events")
                    .and_then(|ev| ev.as_array())
                    .into_iter()
                    .flatten()
                    .filter_map(|e| e.get("ElementActivating"))
                    .any(|b| {
                        b.get("element_id").and_then(|x| x.as_str()) == Some(element)
                            && b.get("instance_key").and_then(|x| x.as_u64()) != Some(instance)
                    })
        })
    };
    let (x, scenario, k1, k2) = entries
        .iter()
        .enumerate()
        .find_map(|(i, r)| {
            let acts = activations(r);
            let (k1, e1) = acts.iter().find(|(k, _)| referenced_later(i, *k))?.clone();
            let (k2, e2) = acts.iter().find(|(k, e)| *k != k1 && *e != e1)?.clone();
            let instance_of = |key: u64| {
                r.get("events")
                    .and_then(|ev| ev.as_array())
                    .into_iter()
                    .flatten()
                    .filter_map(|e| e.get("ElementActivating"))
                    .find(|b| b.get("element_instance_key").and_then(|k| k.as_u64()) == Some(key))
                    .and_then(|b| b.get("instance_key"))
                    .and_then(|k| k.as_u64())
            };
            let i2 = instance_of(k2)?;
            if minted_earlier(i, &e2, i2) {
                return None; // a cross-side identity collision; keep looking
            }
            Some((i, scenarios[i].clone(), k1, k2))
        })
        .expect("an entry minting two elements, one referenced later");
    // prev = head with k1 <-> k2 swapped in entry X ONLY: a clean, ratified
    // remint there, but later entries' raw references to k1 now name the
    // other element on the prev side.
    let mut prev = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[x].get_mut("events") {
        for e in events.iter_mut() {
            for body in e.as_object_mut().into_iter().flat_map(|o| o.values_mut()) {
                if let Some(k) = body.get_mut("element_instance_key") {
                    if *k == serde_json::json!(k1) {
                        *k = serde_json::json!(k2);
                    } else if *k == serde_json::json!(k2) {
                        *k = serde_json::json!(k1);
                    }
                }
            }
        }
    }
    let allow = serde_json::json!({"divergences": [{
        "scenario": scenario,
        "expected": {"reorder": true, "remint_keys": true},
        "introduced_in": "v0.0.2", "issue": "#0",
        "upgrade": "upgrade-boundary", "rationale": "upgrade-boundary: test"
    }]});
    let err = checked(&head, &prev, allow, &[]).unwrap_err();
    assert!(err.contains("entry"), "{err}");
}

/// #1700 review: the keys of ANY String-keyed map field (here the plain
/// `String`-valued `ServiceTask.custom_headers`, which `is_value_map` does
/// not recognise) are DATA — never shape tokens, never erased.
#[test]
fn string_valued_map_keys_are_never_fields() {
    let v = serde_json::json!({"ServiceTask": {
        "job_type": "t", "custom_headers": {"retry_policy": "x"}
    }});
    let mut vocab = compare::Vocab::new();
    compare::shape_tokens(&v, &mut vocab);
    assert!(vocab.contains("ServiceTask.custom_headers"), "{vocab:?}");
    assert!(!vocab.contains("ServiceTask.retry_policy"), "{vocab:?}");

    let mut erased = v.clone();
    let erasable: compare::Vocab = ["ServiceTask.retry_policy".to_owned()].into();
    compare::erase_fields(&mut erased, "$", &erasable);
    assert_eq!(erased, v, "a header named like an added field must survive");

    // Struct-valued maps are still descended (their values ARE structs).
    let mut nested = serde_json::json!({"Snap": {"processes": {"p1": {"gone": 1, "kept": 2}}}});
    let erasable: compare::Vocab = ["Snap.gone".to_owned()].into();
    compare::erase_fields(&mut nested, "$", &erasable);
    assert_eq!(
        nested,
        serde_json::json!({"Snap": {"processes": {"p1": {"kept": 2}}}})
    );
}

/// Drift guard: every String-keyed map field declared ANYWHERE in
/// engine-core/src is covered by `compare::data_map_fields` (derived from
/// `ENGINE_TYPE_SOURCES`); a map declared in an unlisted file fails here.
#[test]
fn data_map_fields_cover_every_engine_map_field() {
    fn walk(dir: &std::path::Path, out: &mut BTreeSet<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n != "tests") {
                    walk(&path, out);
                }
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.extend(compare::map_fields_in(
                    &std::fs::read_to_string(&path).unwrap(),
                ));
            }
        }
    }
    let mut all = BTreeSet::new();
    walk(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut all,
    );
    let missing: Vec<_> = all.difference(compare::data_map_fields()).collect();
    assert!(
        missing.is_empty(),
        "map fields not covered by ENGINE_TYPE_SOURCES (add their file): {missing:?}"
    );
}

/// #1700: every allowlist entry declares how a cluster may upgrade across it
/// — exactly `feature-gated` or `upgrade-boundary`, no default — and its
/// rationale names that mode.
#[test]
fn allowlist_entries_declare_upgrade_mode() {
    let base = |upgrade: Option<&str>, rationale: &str| {
        let mut d = serde_json::json!({
            "scenario": "s", "expected": {}, "introduced_in": "v0.0.2",
            "issue": "#0", "rationale": rationale
        });
        if let Some(u) = upgrade {
            d["upgrade"] = serde_json::json!(u);
        }
        serde_json::json!({"divergences": [d]}).to_string()
    };
    assert!(
        compare::parse_allowlist(&base(None, "x")).is_err(),
        "missing mode"
    );
    assert!(
        compare::parse_allowlist(&base(Some("rolling"), "x")).is_err(),
        "unknown mode"
    );
    for mode in ["feature-gated", "upgrade-boundary"] {
        let a = compare::parse_allowlist(&base(Some(mode), &format!("{mode}: why"))).unwrap();
        assert_eq!(
            a.upgrade_boundaries().len(),
            usize::from(mode == "upgrade-boundary")
        );
    }

    // A rationale that does not name its mode is rejected by the gate.
    let head = head_records();
    let scenario = "jobs-and-user-tasks";
    let i = reorderable_entry(&head, scenario);
    let mut prev = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[i].get_mut("events") {
        events.reverse();
    }
    let allow = serde_json::json!({"divergences": [{
        "scenario": scenario, "expected": {"reorder": true}, "introduced_in": "v0.0.2",
        "issue": "#0", "upgrade": "feature-gated", "rationale": "safe for a rolling upgrade"
    }]});
    let err = checked(&head, &prev, allow, &[]).unwrap_err();
    assert!(err.contains("rationale does not say so"), "{err}");

    // The checked-in allowlist: every current entry is an upgrade boundary
    // (none is feature-gated yet) — what the release notes must call out.
    let real = compare::parse_allowlist(include_str!(
        "../examples/determinism/divergence-allowlist.json"
    ))
    .unwrap();
    assert_eq!(
        real.upgrade_boundaries().len(),
        real.observational_variants.len() + real.divergences.len()
    );
}

/// #1700 review: job identities are one-to-one across the corpus. Two jobs
/// for the same element (in separate entries) must not canonicalize to one
/// identity: pointing a completion at ANOTHER already-minted same-element
/// job key diverges, even under an allowlisted `remint_keys` scenario.
#[test]
fn prev_release_gate_remint_rejects_other_same_element_job_key() {
    let head = head_records();
    let scenarios = labels();
    let entries: Vec<&serde_json::Value> =
        head.iter().filter(|r| r.get("state").is_none()).collect();
    let events_of = |r: &serde_json::Value| -> Vec<serde_json::Value> {
        r.get("events")
            .and_then(|e| e.as_array())
            .cloned()
            .unwrap_or_default()
    };
    // job_key -> element_id over the whole corpus, in mint order.
    let mut jobs: Vec<(u64, String)> = Vec::new();
    let mut found = None;
    'outer: for (i, r) in entries.iter().enumerate() {
        for e in events_of(r) {
            if let Some(b) = e.get("JobCreated") {
                let k = b["job_key"].as_u64().unwrap();
                let el = b["element_id"].as_str().unwrap().to_owned();
                jobs.push((k, el));
            }
            if let Some(b) = e.get("JobCompleted") {
                let k = b["job_key"].as_u64().unwrap();
                let Some((_, el)) = jobs.iter().find(|(jk, _)| *jk == k) else {
                    continue;
                };
                if let Some((other, _)) = jobs.iter().find(|(jk, e2)| *jk != k && e2 == el) {
                    found = Some((i, k, *other));
                    break 'outer;
                }
            }
        }
    }
    let (i, k, other) = found.expect("a completion of a job whose element has another job");
    let mut prev = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[i].get_mut("events") {
        for e in events.iter_mut() {
            if let Some(b) = e.get_mut("JobCompleted") {
                if b["job_key"] == serde_json::json!(k) {
                    b["job_key"] = serde_json::json!(other);
                }
            }
        }
    }
    let allow = serde_json::json!({"divergences": [{
        "scenario": scenarios[i],
        "expected": {"reorder": true, "remint_keys": true},
        "introduced_in": "v0.0.2", "issue": "#0",
        "upgrade": "upgrade-boundary", "rationale": "upgrade-boundary: test"
    }]});
    let err = checked(&head, &prev, allow, &[]).unwrap_err();
    assert!(err.contains("entry"), "{err}");
}

/// #1700 review: a map keyed by a type ALIAS (`ProcessDefinition.elements`,
/// `BTreeMap<ElementId, Element>`) is a data boundary: an element id is never
/// a shape token and never erased, even when named like an added field.
/// Pinned by name, independent of the source scanner.
#[test]
fn aliased_map_keys_are_never_fields() {
    assert!(compare::data_map_fields().contains("elements"));
    let v = serde_json::json!({"ProcessDeployed": {"process": {
        "elements": {"slow": {"id": "slow", "kind": "StartEvent"}}
    }}});
    let mut vocab = compare::Vocab::new();
    compare::shape_tokens(&v, &mut vocab);
    assert!(!vocab.contains("ProcessDeployed.slow"), "{vocab:?}");
    let mut erased = v.clone();
    let erasable: compare::Vocab = ["ProcessDeployed.slow".to_owned()].into();
    compare::erase_fields(&mut erased, "$", &erasable);
    assert_eq!(
        erased, v,
        "an element named like an added field must survive"
    );
}

/// #1700 review: unit variants nested under another variant (an element
/// `kind` of `StartEvent` inside `DeployProcess`) are recorded in the shape
/// vocabulary; arbitrary application strings are not.
#[test]
fn nested_unit_variants_are_shape_tokens() {
    let v = serde_json::json!({"DeployProcess": {
        "name": "Hello", "elements": {"s": {"kind": "StartEvent"}},
        "gw": {"kind": "ParallelGateway"}
    }});
    let mut vocab = compare::Vocab::new();
    compare::shape_tokens(&v, &mut vocab);
    assert!(vocab.contains("StartEvent"), "{vocab:?}");
    assert!(vocab.contains("ParallelGateway"), "{vocab:?}");
    assert!(!vocab.contains("Hello"), "{vocab:?}");
}

/// #1700 review: `ElementActivating`/`ElementActivated.scope` is an
/// element-instance key, so it is canonicalized to its entity identity like
/// any `*_key` field. A cross-entry ratified remint reconciles a scope
/// reference whose raw value changed with its mint; an un-minted scope still
/// diverges (fail closed).
#[test]
fn prev_release_gate_remint_normalizes_scope_across_entries() {
    let head = head_records();
    let scenarios = labels();
    let entries: Vec<&serde_json::Value> =
        head.iter().filter(|r| r.get("state").is_none()).collect();
    let events_of = |r: &serde_json::Value| -> Vec<serde_json::Value> {
        r.get("events")
            .and_then(|e| e.as_array())
            .cloned()
            .unwrap_or_default()
    };
    // (referencing entry, scope key): an `ElementActivating` whose non-root
    // `scope` was minted by an EARLIER entry.
    let mut found = None;
    'outer: for (i, r) in entries.iter().enumerate() {
        for e in events_of(r) {
            let Some(b) = e.get("ElementActivating") else {
                continue;
            };
            let Some(scope) = b.get("scope").and_then(|s| s.as_u64()) else {
                continue;
            };
            if scope == 0 {
                continue;
            }
            let minted_earlier = entries[..i].iter().any(|r| {
                events_of(r).iter().any(|e| {
                    e.get("ElementActivating")
                        .or_else(|| e.get("ElementActivated"))
                        .and_then(|b| b.get("element_instance_key"))
                        .and_then(|k| k.as_u64())
                        == Some(scope)
                })
            });
            if minted_earlier {
                found = Some((i, scope));
                break 'outer;
            }
        }
    }
    let (i, scope) = found.expect("an entry whose scope an earlier entry minted");
    let scenario = scenarios[i].as_str();
    let allow = serde_json::json!({"divergences": [{
        "scenario": scenario,
        "expected": {"reorder": true, "remint_keys": true},
        "introduced_in": "v0.0.2", "issue": "#0",
        "upgrade": "upgrade-boundary", "rationale": "upgrade-boundary: test"
    }]});

    // A clean remint on the prev side: a fresh key namespace for the
    // PROCESS-INSTANCE keys of the referencing entry and its minting entry —
    // every non-zero element-instance key reference (`element_instance_key`,
    // `scope`, `scope_key`, `container_key`, …) offset by the same delta, so
    // the raw scope differs from head's while naming the SAME entity.
    // Without scope normalization this diverges.
    let mint_entry = |entries: &[&serde_json::Value], scope: u64| {
        entries.iter().position(|r| {
            events_of(r).iter().any(|e| {
                e.get("ElementActivating")
                    .or_else(|| e.get("ElementActivated"))
                    .and_then(|b| b.get("element_instance_key"))
                    .and_then(|k| k.as_u64())
                    == Some(scope)
            })
        })
    };
    let j = mint_entry(&entries, scope).expect("the scope's minting entry");
    // The process instance the scope's element instance belongs to.
    let instance_key = entries[j]
        .get("events")
        .and_then(|e| e.as_array())
        .into_iter()
        .flatten()
        .filter_map(|e| {
            e.get("ElementActivating")
                .or_else(|| e.get("ElementActivated"))
        })
        .find(|b| b.get("element_instance_key").and_then(|k| k.as_u64()) == Some(scope))
        .and_then(|b| b.get("instance_key"))
        .and_then(|k| k.as_u64())
        .expect("the minting event's instance_key");
    let mut prev = head.clone();
    for r in entries_mut(&mut prev).into_iter().skip(j) {
        if let Some(serde_json::Value::Array(events)) = r.get_mut("events") {
            for e in events.iter_mut() {
                for body in e.as_object_mut().into_iter().flat_map(|o| o.values_mut()) {
                    // Only this instance's own events: another instance's
                    // keys are not re-minted here.
                    if body.get("instance_key").and_then(|k| k.as_u64()) != Some(instance_key) {
                        continue;
                    }
                    for (k, v) in body.as_object_mut().into_iter().flat_map(|o| o.iter_mut()) {
                        let is_instance_ref = k == "element_instance_key"
                            || k == "scope"
                            || k == "scope_key"
                            || k == "parent_scope_key"
                            || k == "container_key"
                            || k == "child_key";
                        if is_instance_ref && v.as_u64().is_some_and(|n| n != 0) {
                            *v = serde_json::json!(v.as_u64().unwrap() + 500_000);
                        }
                    }
                }
            }
        }
    }
    let report = checked(&head, &prev, allow.clone(), &[]).unwrap();
    assert!(
        report.ratified_divergent_entries > 0,
        "a clean remint with a cross-entry scope reference must reconcile"
    );

    // The fail-closed half: point entry i's scope at a key the corpus never
    // minted. Identity canonicalization turns it into a distinct orphan
    // token, so the gate fails.
    let mut broken = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut broken)[i].get_mut("events") {
        for e in events.iter_mut() {
            for body in e.as_object_mut().into_iter().flat_map(|o| o.values_mut()) {
                if body.get("scope").and_then(|s| s.as_u64()) == Some(scope) {
                    body["scope"] = serde_json::json!(9_999_999u64);
                }
            }
        }
    }
    let err = checked(&head, &broken, allow, &[]).unwrap_err();
    assert!(
        err.contains("entry"),
        "an un-minted scope must diverge: {err}"
    );
}

/// #1700 review: `EngineSnapshot`/`State` are untagged structs, so their
/// fields keep the `$` context. An explicitly listed `$`-qualified field
/// (e.g. `$.raft_applied`, additive since snapshot v2) IS erased — while
/// the map-data boundaries (variable names, element ids) still are not.
#[test]
fn prev_release_gate_erases_listed_root_state_fields() {
    // Unit: a `$`-qualified token erases at the root; it does NOT erase a
    // same-named field under a variant (event fields are variant-qualified,
    // so no `$` token ever names one).
    let mut snap = serde_json::json!({"raft_applied": [7, "s"], "now": 3});
    let erasable: compare::Vocab = ["$.raft_applied".to_owned()].into();
    compare::erase_fields(&mut snap, "$", &erasable);
    assert_eq!(snap, serde_json::json!({"now": 3}));
    let mut ev = serde_json::json!({"JobCanceled": {"raft_applied": 9}});
    compare::erase_fields(&mut ev, "$", &erasable);
    assert_eq!(ev, serde_json::json!({"JobCanceled": {"raft_applied": 9}}));

    // Snapshot regression: a populated golden-added non-default field on the
    // head side, absent on the prev side, reconciles when listed; a
    // NON-listed divergence still fails.
    let mut engine = Engine::new();
    engine.set_raft_applied(7, "opaque".to_owned());
    let head_state = serde_json::to_value(engine.snapshot()).unwrap();
    assert!(head_state.get("raft_applied").is_some());
    let mut prev_state = head_state.clone();
    prev_state.as_object_mut().unwrap().remove("raft_applied");
    let rec = |state: serde_json::Value| serde_json::json!({"i": 0, "format": nanobpmn_engine_core::SNAPSHOT_FORMAT_VERSION, "state": state});
    let one_entry = serde_json::json!({"i": 0, "err": "boom"});
    let head = vec![one_entry.clone(), rec(head_state)];
    let prev = vec![one_entry, rec(prev_state)];
    let one_scenario = |added: &[&str]| {
        let scenarios = vec!["synthetic".to_owned()];
        let allowlist = compare::parse_allowlist("{}").unwrap();
        let added: compare::Vocab = added.iter().map(|t| (*t).to_owned()).collect();
        let ctx = compare::CrossReleaseContext {
            scenarios: &scenarios,
            allowlist: &allowlist,
            prev_tag: "v0.0.1",
            added: &added,
        };
        compare::cross_release_checked(&head, &prev, &ctx)
    };
    let err = one_scenario(&[]).unwrap_err();
    assert!(err.contains("durable state diverges"), "{err}");
    let report = one_scenario(&["$.raft_applied"]).unwrap();
    assert_eq!(report.base.states_compared, 1);
    assert_eq!(report.erased_fields, ["$.raft_applied".to_owned()]);

    // Map-data boundaries are retained at the root too: a `$`-qualified
    // token never erases a same-named map KEY (e.g. a process id `now`
    // inside the `processes` map).
    let mut with_map = serde_json::json!({"processes": {"now": {"key": 1}}, "now": 3});
    let erasable_now: compare::Vocab = ["$.now".to_owned()].into();
    compare::erase_fields(&mut with_map, "$", &erasable_now);
    assert_eq!(
        with_map,
        serde_json::json!({"processes": {"now": {"key": 1}}}),
        "the listed root field is erased but the same-named map key survives"
    );
}

/// #1700 (maintainer): a ratified `reorder` divergence may MOVE an event
/// across entries — #1244 activates a parallel join's element instance when
/// the join FIRES, so its `ElementActivating`/`ElementActivated` leave the
/// first arriving token's entry and appear in the firing entry. Per-entry
/// `only_in_prev`/`only_in_head` matching cannot reconcile that, so the
/// patterns reconcile at SCENARIO level: the multiset of matched events over
/// the whole scenario must balance (each pattern removed from prev appears in
/// head, same scenario, same element, same instance after remint) while every
/// entry stays strict per entry otherwise.
#[test]
fn prev_release_gate_ratified_patterns_reconcile_across_entries() {
    let head = head_records();
    let scenario = "messages-signals-conditional";
    let scenarios = labels();
    let join_activation = |e: &serde_json::Value| {
        ["ElementActivating", "ElementActivated"].iter().any(|v| {
            e.get(v)
                .and_then(|b| b.get("element_id"))
                .and_then(|id| id.as_str())
                == Some("join")
        })
    };
    let entries: Vec<&serde_json::Value> =
        head.iter().filter(|r| r.get("state").is_none()).collect();
    let has_join_activation = |r: &serde_json::Value| {
        r.get("events")
            .and_then(|e| e.as_array())
            .is_some_and(|evs| evs.iter().any(join_activation))
    };
    // The scenario must exercise a join activation at all, and a LATER entry
    // of the same scenario must exist to move it into (the #1244 shape: the
    // join fires on a later command than the first arriving token).
    let from = entries
        .iter()
        .enumerate()
        .find(|(i, r)| scenarios[*i] == scenario && has_join_activation(r))
        .map(|(i, _)| i)
        .expect("a join activation in the scenario");
    let to = (from + 1..entries.len())
        .find(|&i| scenarios[i] == scenario && !has_join_activation(entries[i]))
        .expect("a later entry of the same scenario without a join activation");

    let allow = serde_json::json!({"divergences": [{
        "scenario": scenario,
        "expected": {
            "reorder": true, "remint_keys": true,
            "only_in_prev": [
                {"variant": "ElementActivating", "element_id": "join"},
                {"variant": "ElementActivated", "element_id": "join"}
            ],
            "only_in_head": [
                {"variant": "ElementActivating", "element_id": "join"},
                {"variant": "ElementActivated", "element_id": "join"}
            ]
        },
        "introduced_in": "v0.0.28", "issue": "#1244",
        "upgrade": "upgrade-boundary", "rationale": "upgrade-boundary: test"
    }]});

    // (a) A cross-entry move of the ratified pattern: prev keeps the join
    // activation at entry `from`; head drops it there and emits it (with the
    // same keys) at entry `to`. The scenario-level balance reconciles it.
    let mut moved = head.clone();
    let mut moved_events: Vec<serde_json::Value> = Vec::new();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut moved)[from].get_mut("events")
    {
        moved_events = events.extract_if(.., |e| join_activation(e)).collect();
    }
    assert_eq!(moved_events.len(), 2, "join activating + activated");
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut moved)[to].get_mut("events") {
        events.extend(moved_events.iter().cloned());
    }
    let report = checked(&moved, &head, allow.clone(), &[]).unwrap();
    assert!(
        report.ratified_divergent_entries >= 2,
        "both the source and the target entry diverge and reconcile: {report:?}"
    );

    // (b) The same pattern only REMOVED (never re-added in the scenario): an
    // unbalanced ratified pattern still fails — the guard against hiding a
    // real loss. Head drops the join activation at entry `from` and never
    // re-adds it, so head's divergent entries mint the join one fewer time
    // than prev's: the scenario-level balance reports it unbalanced.
    let mut removed = head.clone();
    if let Some(serde_json::Value::Array(events)) =
        entries_mut(&mut removed)[from].get_mut("events")
    {
        events.retain(|e| !join_activation(e));
    }
    let err = checked(&removed, &head, allow.clone(), &[]).unwrap_err();
    assert!(
        err.contains("unbalanced"),
        "a removed-but-never-re-added pattern must fail as unbalanced: {err}"
    );

    // (d)-(f) Lifecycle and scope identity are preserved by the balance
    // (Copilot, #1700): on top of the valid move (a), deleting the moved
    // `ElementActivated`, duplicating it, or re-scoping the moved
    // `ElementActivating` must each fail as unbalanced.
    let mutate_moved = |f: &dyn Fn(&mut Vec<serde_json::Value>)| {
        let mut m = moved.clone();
        if let Some(serde_json::Value::Array(events)) = entries_mut(&mut m)[to].get_mut("events") {
            f(events);
        }
        m
    };
    let is = |e: &serde_json::Value, v: &str| {
        e.get(v)
            .and_then(|b| b.get("element_id"))
            .and_then(|id| id.as_str())
            == Some("join")
    };
    let deleted = mutate_moved(&|evs| evs.retain(|e| !is(e, "ElementActivated")));
    let duplicated = mutate_moved(&|evs| {
        let dup = evs.iter().find(|e| is(e, "ElementActivated")).cloned();
        evs.extend(dup);
    });
    let rescoped = mutate_moved(&|evs| {
        for e in evs.iter_mut().filter(|e| is(e, "ElementActivating")) {
            if let Some(b) = e
                .get_mut("ElementActivating")
                .and_then(|b| b.as_object_mut())
            {
                b.insert("scope".to_owned(), serde_json::json!(8_888_888u64));
            }
        }
    });
    for (label, mutated) in [
        ("deleted ElementActivated", deleted),
        ("duplicated ElementActivated", duplicated),
        ("re-scoped ElementActivating", rescoped),
    ] {
        assert_ne!(mutated, moved, "{label}: the mutation must apply");
        let err = checked(&mutated, &head, allow.clone(), &[]).unwrap_err();
        assert!(
            err.contains("unbalanced") || err.contains("UNDECLARED"),
            "{label} must fail the gate: {err}"
        );
    }

    // (c) An unrelated EXTRA event in the allowlisted scenario: not matched
    // by any declared pattern, so it fails even with the scenario-level
    // reconciliation in place.
    let mut extra = moved.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut extra)[to].get_mut("events") {
        events.push(serde_json::json!({"JobCanceled": {
            "job_key": 9_999_999u64, "instance_key": 9_999_999u64, "canceled_at": 0
        }}));
    }
    let err = checked(&extra, &head, allow, &[]).unwrap_err();
    assert!(
        err.contains("UNDECLARED extra divergence"),
        "an unrelated extra event must fail: {err}"
    );
}

/// Drift guard: every unit variant declared anywhere in engine-core/src is
/// covered by `compare::unit_variants` (derived from `ENGINE_TYPE_SOURCES`).
#[test]
fn unit_variants_cover_every_engine_unit_variant() {
    fn walk(dir: &std::path::Path, out: &mut BTreeSet<String>) {
        for entry in std::fs::read_dir(dir).unwrap() {
            let path = entry.unwrap().path();
            if path.is_dir() {
                if path.file_name().is_some_and(|n| n != "tests") {
                    walk(&path, out);
                }
            } else if path.extension().is_some_and(|e| e == "rs") {
                out.extend(compare::unit_variants_in(
                    &std::fs::read_to_string(&path).unwrap(),
                ));
            }
        }
    }
    let mut all = BTreeSet::new();
    walk(
        &std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src"),
        &mut all,
    );
    let missing: Vec<_> = all.difference(compare::unit_variants()).collect();
    assert!(
        missing.is_empty(),
        "unit variants not covered by ENGINE_TYPE_SOURCES (add their file): {missing:?}"
    );
}
