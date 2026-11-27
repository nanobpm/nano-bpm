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
    let substituted = entries_mut(&mut head.clone())[i]["events"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|e| e.get("ElementCompleted").is_some())
        .count();
    let allow_with = |count: Option<usize>| {
        // Both lifecycle kinds on both sides, as the real MI cancel entry
        // declares (the patterns cannot single out the substituted events).
        let both =
            serde_json::json!([{"variant": "ElementTerminated"}, {"variant": "ElementCompleted"}]);
        let mut expected = serde_json::json!({
            "only_in_prev": both.clone(),
            "only_in_head": both,
            "variant_substitutions": [{"prev": "ElementTerminated", "head": "ElementCompleted"}]
        });
        if let Some(n) = count {
            expected["substituted_head_events"] = serde_json::json!(n);
        }
        serde_json::json!({"divergences": [{
            "scenario": scenario,
            "expected": expected,
            "introduced_in": "v0.0.2", "issue": "#0", "upgrade": "upgrade-boundary",
            "rationale": "upgrade-boundary: test"
        }]})
    };
    let report = checked(
        &head,
        &prev,
        allow_with(Some(substituted)),
        &["ElementTerminated"],
    )
    .unwrap();
    assert_eq!(report.ratified_divergent_entries, 1);
    // #1528 review: a substitution without the pinned head-form count fails
    // closed (a regression to the prev kind would otherwise balance).
    let err = checked(&head, &prev, allow_with(None), &["ElementTerminated"]).unwrap_err();
    assert!(err.contains("substituted_head_events"), "{err}");
    // #1528 review: ONE substituted event regressing to the prev lifecycle
    // kind on head still balances under the projection, but the pinned count
    // catches it.
    let mut regressed = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut regressed)[i].get_mut("events")
    {
        let e = events
            .iter_mut()
            .find(|e| e.get("ElementCompleted").is_some())
            .unwrap();
        let obj = e.as_object_mut().unwrap();
        let body = obj.remove("ElementCompleted").unwrap();
        obj.insert("ElementTerminated".to_owned(), body);
    }
    let err = checked(
        &regressed,
        &prev,
        allow_with(Some(substituted)),
        &["ElementTerminated"],
    )
    .unwrap_err();
    assert!(err.contains("regressed"), "{err}");

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

    // The scalar-list routing deployment (#1755): its generation-time
    // routing assertion must not panic once the deployment is excluded.
    let sr_deploy = position_of(&full, "deploy_process")
        .into_iter()
        .find(
            |&p| matches!(&full[p as usize].1, Command::DeployProcess(d) if d.id == "scalar-route"),
        )
        .expect("scalar-route deploy");
    let (_, _, excluded) = corpus::build_excluding(RANDOM, [sr_deploy].into()).unwrap();
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

/// #1528 (review): a golden-added field nested under a VARIANT's map field
/// (a `ProcessDeployed` process's `elements` map, whose values are element
/// structs) must be erased under that variant's context — not left in place
/// because the map walk kept the stale `$` root context. This is the
/// divergence the previous-release gate reports on every deploy when a
/// release adds a per-element field (e.g. `properties`).
#[test]
fn erasure_reaches_fields_under_a_variant_map_field() {
    // ProcessDeployed -> process -> elements -> {id} -> properties.
    let deploy = serde_json::json!({"ProcessDeployed": {"process": {
        "id": "p",
        "elements": {"task": {"id": "task", "properties": {}, "kind": "x"}}
    }}});
    let mut erased = deploy.clone();
    let erasable: compare::Vocab = ["ProcessDeployed.properties".to_owned()].into();
    compare::erase_fields(&mut erased, "$", &erasable);
    assert_eq!(
        erased,
        serde_json::json!({"ProcessDeployed": {"process": {
            "id": "p",
            "elements": {"task": {"id": "task", "kind": "x"}}
        }}}),
        "a golden-added field under a variant's map field must be erased"
    );

    // A field the golden does NOT record as added under that variant stays —
    // the erasure never over-reaches into map data it cannot name.
    let mut kept = deploy.clone();
    let erasable: compare::Vocab = ["ProcessDeployed.name".to_owned()].into();
    compare::erase_fields(&mut kept, "$", &erasable);
    assert_eq!(
        kept, deploy,
        "only the listed field is erased; the element id/kind survive"
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

/// #1528: an UNBALANCED scenario divergence (the change adds events with no
/// counterpart on the previous release — e.g. an MI user task's
/// `UserTaskCreated` per child, which the old build never emits) is ratified
/// only by an explicit, scenario-scoped `unbalanced` declaration that names
/// its issue and reason. (a) An undeclared unbalanced diff still FAILS;
/// (b) a declared one passes only for its named scenario — the same diff in
/// another scenario still fails; (c) a declaration without a reason or an
/// issue is rejected.
#[test]
fn prev_release_gate_unbalanced_divergence_requires_an_explicit_declaration() {
    let head = head_records();
    let scenario = "jobs-and-user-tasks";
    // The synthetic previous release: `head` with the scenario's
    // `UserTaskAssigned` events removed (a release that never emits them
    // there) — an imbalance, not a move. Dropped ONLY in `scenario` (the
    // corpus emits `UserTaskAssigned` in other scenarios too, and those must
    // keep agreeing). The variant is unknown to `added`, so the
    // new-variant pre-filter cannot fire — the scenario comparison is what
    // is exercised.
    let scenarios = labels();
    let mut prev = head.clone();
    let mut dropped = 0;
    for (idx, r) in entries_mut(&mut prev).into_iter().enumerate() {
        if scenarios[idx] != scenario {
            continue;
        }
        if let Some(serde_json::Value::Array(events)) = r.get_mut("events") {
            let before = events.len();
            events.retain(|e| e.get("UserTaskAssigned").is_none());
            dropped += before - events.len();
        }
    }
    assert!(
        dropped > 0,
        "the corpus must emit UserTaskAssigned in {scenario}"
    );
    let declare = |expected: serde_json::Value, issue: &str| {
        serde_json::json!({"divergences": [{
            "scenario": scenario, "expected": expected, "introduced_in": "v0.0.2",
            "issue": issue, "upgrade": "upgrade-boundary",
            "rationale": "upgrade-boundary: test"
        }]})
    };
    let patterns = serde_json::json!({"only_in_head": [{"variant": "UserTaskAssigned"}]});

    // (a) Undeclared: the imbalance fails — as a missing declaration, and as
    // an unbalanced ratified pattern when the patterns are declared without
    // the `unbalanced` marker.
    let err = checked(&head, &prev, no_allowlist(), &[]).unwrap_err();
    assert!(err.contains(&format!("scenario {scenario}")), "{err}");
    let err = checked(&head, &prev, declare(patterns.clone(), "#0"), &[]).unwrap_err();
    assert!(err.contains("unbalanced"), "{err}");

    // (c) A declaration without a reason — or a divergence without an issue —
    // is rejected: the ratification must be traceable to a reviewed decision.
    let no_reason = serde_json::json!({
        "only_in_head": [{"variant": "UserTaskAssigned"}],
        "unbalanced": {"reason": "  "}
    });
    let err = checked(&head, &prev, declare(no_reason, "#0"), &[]).unwrap_err();
    assert!(err.contains("non-empty reason AND a named issue"), "{err}");
    let no_issue = serde_json::json!({
        "only_in_head": [{"variant": "UserTaskAssigned"}],
        "unbalanced": {"reason": "a real reason"}
    });
    let err = checked(&head, &prev, declare(no_issue, "  "), &[]).unwrap_err();
    assert!(err.contains("non-empty reason AND a named issue"), "{err}");

    // (b) Declared, with issue and reason: the named scenario passes.
    let declared = serde_json::json!({
        "only_in_head": [{"variant": "UserTaskAssigned"}],
        "unbalanced": {"reason": "a real reason"}
    });
    let report = checked(&head, &prev, declare(declared.clone(), "#1528"), &[]).unwrap();
    assert!(report.ratified_divergent_entries > 0);

    // (b) … and ONLY the named scenario: declaring a DIFFERENT scenario
    // leaves this one undeclared, so the same diff fails.
    let wrong_scenario = serde_json::json!({"divergences": [{
        "scenario": "messages-signals-conditional",
        "expected": {
            "only_in_head": [{"variant": "UserTaskAssigned"}],
            "unbalanced": {"reason": "a real reason"}
        },
        "introduced_in": "v0.0.2", "issue": "#1528", "upgrade": "upgrade-boundary",
        "rationale": "upgrade-boundary: test"
    }]});
    let err = checked(&head, &prev, wrong_scenario, &[]).unwrap_err();
    assert!(
        err.contains(&format!("scenario {scenario}")),
        "an unbalanced declaration is scenario-scoped; {scenario} must fail when only another scenario is declared: {err}"
    );
}

/// The index (among the non-state entry records) of the
/// `multi-instance-user-tasks` CREATE entry — the one whose events carry the
/// MI body, its children, and the per-child `UserTaskCreated` events.
fn mi_create_entry(records: &[serde_json::Value]) -> usize {
    entry_emitting(
        records,
        "multi-instance-user-tasks",
        "MultiInstanceChildActivated",
    )
}

/// The `multi-instance-user-tasks` divergence declaration (`remint_keys` +
/// the unbalanced per-child `UserTaskCreated`), mirroring the allowlist.
fn mi_declaration() -> serde_json::Value {
    serde_json::json!({"divergences": [{
        "scenario": "multi-instance-user-tasks",
        "expected": {
            "reorder": true,
            "remint_keys": true,
            "only_in_head": [{"variant": "UserTaskCreated"}],
            "only_in_prev": [
                {"variant": "ElementActivated", "element_id": "review"},
                {"variant": "ElementCompleted", "element_id": "review"},
                {"variant": "SequenceFlowTaken", "from": "review"}
            ],
            "unbalanced": {"reason": "Zeebe parity: an MI userTask child creates a real user task"}
        },
        "introduced_in": "v0.0.2", "issue": "#1528", "upgrade": "upgrade-boundary",
        "rationale": "upgrade-boundary: test"
    }]})
}

/// #1528 (review): an MI child's canonical identity is independent of the
/// entry's event order — whether its generic `ElementActivating` or its
/// `MultiInstanceChildActivated` is seen first, the child canonicalizes to the
/// same `mi-child:…` identity, so a pure reorder reconciles.
#[test]
fn prev_release_gate_mi_child_identity_is_order_independent() {
    // A self-contained entry: an instance whose MI body (element `review`)
    // activates one child. `prev` is the SAME events reversed — a pure
    // reorder (same keys, permuted order), so the child's generic
    // `ElementActivating` now precedes its `MultiInstanceChildActivated`. With
    // `reorder` + `remint_keys` declared, the two must reconcile: the child's
    // canonical identity is the same whichever sighting comes first.
    let entry = |order: &[&str]| -> serde_json::Value {
        let events: Vec<serde_json::Value> = order
            .iter()
            .map(|kind| match *kind {
                "create" => serde_json::json!({"ProcessInstanceCreated":{"business_id":null,"created_at":1,"instance_key":10,"parent_element_instance_key":null,"parent_process_instance_key":null,"process_definition_key":9,"process_id":"p","tags":[],"variables":{},"version":1}}),
                "body_activating" => serde_json::json!({"ElementActivating":{"element_id":"review","element_instance_key":11,"instance_key":10,"scope":0}}),
                "child_activating" => serde_json::json!({"ElementActivating":{"element_id":"review","element_instance_key":12,"instance_key":10,"scope":11}}),
                "child_activated" => serde_json::json!({"MultiInstanceChildActivated":{"body_key":11,"child_key":12,"index":0,"instance_key":10,"local_variables":{}}}),
                other => panic!("unknown event {other}"),
            })
            .collect();
        serde_json::json!({"i": 0, "events": events})
    };
    let forward = entry(&[
        "create",
        "body_activating",
        "child_activating",
        "child_activated",
    ]);
    let reversed = entry(&[
        "child_activated",
        "child_activating",
        "body_activating",
        "create",
    ]);
    // One state checkpoint each (same format) so the state comparison is live —
    // a real snapshot from a corpus run, so it decodes under the head type.
    let real_state = head_records()
        .into_iter()
        .find(|r| r.get("state").is_some())
        .expect("a state checkpoint");
    let state = || {
        let mut s = real_state.clone();
        s["i"] = serde_json::json!(1);
        s
    };
    let head = vec![forward.clone(), state()];
    let prev = vec![reversed, state()];
    let allow = serde_json::json!({"divergences": [{
        "scenario": "s", "expected": {"reorder": true, "remint_keys": true},
        "introduced_in": "v0.0.2", "issue": "#1528", "upgrade": "upgrade-boundary",
        "rationale": "upgrade-boundary: test"
    }]});
    let scenarios = vec!["s".to_string()];
    let allowlist = compare::parse_allowlist(&allow.to_string()).unwrap();
    let added = compare::Vocab::new();
    let ctx = compare::CrossReleaseContext {
        scenarios: &scenarios,
        allowlist: &allowlist,
        prev_tag: "v0.0.1",
        added: &added,
    };
    compare::cross_release_checked(&head, &prev, &ctx)
        .expect("a pure reorder of an MI child's sightings reconciles");
}

/// #1528 (review): a NESTED multi-instance — the same inner MI element active
/// under several outer children — activates one inner body per outer child, and
/// every inner body's generic activation names the same (instance, element id).
/// An encounter-order ordinal on that base would swap the two bodies'
/// identities when a ratified `reorder` permutes the entry, failing a valid
/// `reorder` + `remint_keys` comparison; the bodies' identities must be derived
/// from their canonical PARENT SCOPE (the outer child) instead, so the reorder
/// reconciles.
#[test]
fn prev_release_gate_nested_mi_body_identity_is_order_independent() {
    // A self-contained entry: an instance whose outer MI body (element `outer`)
    // activates two children, each activating an inner MI body (element
    // `inner`) with one child of its own. `prev` is the SAME events reversed —
    // a pure reorder — so the inner bodies' activations arrive in the opposite
    // order. With `reorder` + `remint_keys` declared, the two must reconcile:
    // each inner body's canonical identity is derived from its outer child's
    // identity, not from which body activation was folded first.
    let entry = |order: &[&str]| -> serde_json::Value {
        let events: Vec<serde_json::Value> = order
            .iter()
            .map(|kind| match *kind {
                "create" => serde_json::json!({"ProcessInstanceCreated":{"business_id":null,"created_at":1,"instance_key":10,"parent_element_instance_key":null,"parent_process_instance_key":null,"process_definition_key":9,"process_id":"p","tags":[],"variables":{},"version":1}}),
                "outer_body" => serde_json::json!({"ElementActivating":{"element_id":"outer","element_instance_key":11,"instance_key":10,"scope":0}}),
                "outer_child0" => serde_json::json!({"ElementActivating":{"element_id":"outer","element_instance_key":12,"instance_key":10,"scope":11}}),
                "outer_child0_mi" => serde_json::json!({"MultiInstanceChildActivated":{"body_key":11,"child_key":12,"index":0,"instance_key":10,"local_variables":{}}}),
                "outer_child1" => serde_json::json!({"ElementActivating":{"element_id":"outer","element_instance_key":13,"instance_key":10,"scope":11}}),
                "outer_child1_mi" => serde_json::json!({"MultiInstanceChildActivated":{"body_key":11,"child_key":13,"index":1,"instance_key":10,"local_variables":{}}}),
                // The same inner MI element, one body per outer child: both
                // generic activations name (instance 10, element `inner`) — an
                // encounter-order ordinal cannot tell them apart.
                "inner_body0" => serde_json::json!({"ElementActivating":{"element_id":"inner","element_instance_key":14,"instance_key":10,"scope":12}}),
                "inner_child0" => serde_json::json!({"ElementActivating":{"element_id":"inner","element_instance_key":15,"instance_key":10,"scope":14}}),
                "inner_child0_mi" => serde_json::json!({"MultiInstanceChildActivated":{"body_key":14,"child_key":15,"index":0,"instance_key":10,"local_variables":{}}}),
                "inner_body1" => serde_json::json!({"ElementActivating":{"element_id":"inner","element_instance_key":16,"instance_key":10,"scope":13}}),
                "inner_child1" => serde_json::json!({"ElementActivating":{"element_id":"inner","element_instance_key":17,"instance_key":10,"scope":16}}),
                "inner_child1_mi" => serde_json::json!({"MultiInstanceChildActivated":{"body_key":16,"child_key":17,"index":0,"instance_key":10,"local_variables":{}}}),
                other => panic!("unknown event {other}"),
            })
            .collect();
        serde_json::json!({"i": 0, "events": events})
    };
    let forward = entry(&[
        "create",
        "outer_body",
        "outer_child0",
        "outer_child0_mi",
        "inner_body0",
        "inner_child0",
        "inner_child0_mi",
        "outer_child1",
        "outer_child1_mi",
        "inner_body1",
        "inner_child1",
        "inner_child1_mi",
    ]);
    let reversed = entry(&[
        "inner_child1_mi",
        "inner_child1",
        "inner_body1",
        "outer_child1_mi",
        "outer_child1",
        "inner_child0_mi",
        "inner_child0",
        "inner_body0",
        "outer_child0_mi",
        "outer_child0",
        "outer_body",
        "create",
    ]);
    // One state checkpoint each (same format) so the state comparison is live —
    // a real snapshot from a corpus run, so it decodes under the head type.
    let real_state = head_records()
        .into_iter()
        .find(|r| r.get("state").is_some())
        .expect("a state checkpoint");
    let state = || {
        let mut s = real_state.clone();
        s["i"] = serde_json::json!(1);
        s
    };
    let head = vec![forward, state()];
    let prev = vec![reversed, state()];
    let allow = serde_json::json!({"divergences": [{
        "scenario": "s", "expected": {"reorder": true, "remint_keys": true},
        "introduced_in": "v0.0.2", "issue": "#1528", "upgrade": "upgrade-boundary",
        "rationale": "upgrade-boundary: test"
    }]});
    let scenarios = vec!["s".to_string()];
    let allowlist = compare::parse_allowlist(&allow.to_string()).unwrap();
    let added = compare::Vocab::new();
    let ctx = compare::CrossReleaseContext {
        scenarios: &scenarios,
        allowlist: &allowlist,
        prev_tag: "v0.0.1",
        added: &added,
    };
    compare::cross_release_checked(&head, &prev, &ctx).expect(
        "a pure reorder of a nested loop's body activations reconciles: each inner \
         body's identity is derived from its canonical parent scope (its outer child)",
    );
}

/// Runs the cross-release gate over ONE synthetic entry per side (plus a
/// real state checkpoint so the state comparison is live), with `reorder` +
/// `remint_keys` declared for the scenario.
fn nested_gate(
    head_events: Vec<serde_json::Value>,
    prev_events: Vec<serde_json::Value>,
) -> Result<(), String> {
    let real_state = head_records()
        .into_iter()
        .find(|r| r.get("state").is_some())
        .expect("a state checkpoint");
    let state = || {
        let mut s = real_state.clone();
        s["i"] = serde_json::json!(1);
        s
    };
    let head = vec![serde_json::json!({"i": 0, "events": head_events}), state()];
    let prev = vec![serde_json::json!({"i": 0, "events": prev_events}), state()];
    let allow = serde_json::json!({"divergences": [{
        "scenario": "s", "expected": {"reorder": true, "remint_keys": true},
        "introduced_in": "v0.0.2", "issue": "#1528", "upgrade": "upgrade-boundary",
        "rationale": "upgrade-boundary: test"
    }]});
    let scenarios = vec!["s".to_string()];
    let allowlist = compare::parse_allowlist(&allow.to_string()).unwrap();
    let added = compare::Vocab::new();
    let ctx = compare::CrossReleaseContext {
        scenarios: &scenarios,
        allowlist: &allowlist,
        prev_tag: "v0.0.1",
        added: &added,
    };
    compare::cross_release_checked(&head, &prev, &ctx).map(|_| ())
}

/// A nested MI tree (outer body 11 → children 12, 13 → inner bodies 14, 16 →
/// inner children 15, 17), every element-instance key offset by `shift` (the
/// instance key 10 stays put), each inner child parking one job (key 30+).
fn nested_tree(shift: u64) -> Vec<serde_json::Value> {
    let k = |raw: u64| raw + shift;
    vec![
        serde_json::json!({"ProcessInstanceCreated":{"business_id":null,"created_at":1,"instance_key":10,"parent_element_instance_key":null,"parent_process_instance_key":null,"process_definition_key":9,"process_id":"p","tags":[],"variables":{},"version":1}}),
        serde_json::json!({"ElementActivating":{"element_id":"outer","element_instance_key":k(11),"instance_key":10,"scope":0}}),
        serde_json::json!({"ElementActivating":{"element_id":"outer","element_instance_key":k(12),"instance_key":10,"scope":k(11)}}),
        serde_json::json!({"MultiInstanceChildActivated":{"body_key":k(11),"child_key":k(12),"index":0,"instance_key":10,"local_variables":{}}}),
        serde_json::json!({"ElementActivating":{"element_id":"outer","element_instance_key":k(13),"instance_key":10,"scope":k(11)}}),
        serde_json::json!({"MultiInstanceChildActivated":{"body_key":k(11),"child_key":k(13),"index":1,"instance_key":10,"local_variables":{}}}),
        serde_json::json!({"ElementActivating":{"element_id":"inner","element_instance_key":k(14),"instance_key":10,"scope":k(12)}}),
        serde_json::json!({"ElementActivating":{"element_id":"inner","element_instance_key":k(15),"instance_key":10,"scope":k(14)}}),
        serde_json::json!({"MultiInstanceChildActivated":{"body_key":k(14),"child_key":k(15),"index":0,"instance_key":10,"local_variables":{}}}),
        serde_json::json!({"ElementActivating":{"element_id":"inner","element_instance_key":k(16),"instance_key":10,"scope":k(13)}}),
        serde_json::json!({"ElementActivating":{"element_id":"inner","element_instance_key":k(17),"instance_key":10,"scope":k(16)}}),
        serde_json::json!({"MultiInstanceChildActivated":{"body_key":k(16),"child_key":k(17),"index":0,"instance_key":10,"local_variables":{}}}),
    ]
}

/// #1528 (review): an inner MI body activated in the SAME entry as the outer
/// child that is its parent scope must derive its identity from that child's
/// canonical `mi-child:…` identity, not a raw-key `?orphan:` token — so a
/// clean REMINT of the whole nested tree (every key shifted) reconciles.
/// Registration is parent-before-child to a fixpoint.
#[test]
fn prev_release_gate_nested_mi_tree_reconciles_under_remint() {
    nested_gate(nested_tree(0), nested_tree(100))
        .expect("a reminted nested MI tree reconciles: inner bodies derive from their outer child");
}

/// #1528 (review): two executions of the SAME MI activity in one parent scope
/// (a loop-back) must keep DISTINCT body identities — and so distinct child
/// identities — or `remint_keys` would hide a reference that points to the
/// wrong execution. Swapping which execution's child owns a job must fail.
#[test]
fn prev_release_gate_repeated_mi_body_executions_stay_distinct() {
    let tree = |shift: u64, job_owner: u64| {
        let k = |raw: u64| raw + shift;
        vec![
            serde_json::json!({"ProcessInstanceCreated":{"business_id":null,"created_at":1,"instance_key":10,"parent_element_instance_key":null,"parent_process_instance_key":null,"process_definition_key":9,"process_id":"p","tags":[],"variables":{},"version":1}}),
            // Execution 1 of MI activity `a`.
            serde_json::json!({"ElementActivating":{"element_id":"a","element_instance_key":k(11),"instance_key":10,"scope":0}}),
            serde_json::json!({"ElementActivating":{"element_id":"a","element_instance_key":k(12),"instance_key":10,"scope":k(11)}}),
            serde_json::json!({"MultiInstanceChildActivated":{"body_key":k(11),"child_key":k(12),"index":0,"instance_key":10,"local_variables":{}}}),
            // Execution 2 of the same activity, same parent scope.
            serde_json::json!({"ElementActivating":{"element_id":"a","element_instance_key":k(13),"instance_key":10,"scope":0}}),
            serde_json::json!({"ElementActivating":{"element_id":"a","element_instance_key":k(14),"instance_key":10,"scope":k(13)}}),
            serde_json::json!({"MultiInstanceChildActivated":{"body_key":k(13),"child_key":k(14),"index":0,"instance_key":10,"local_variables":{}}}),
            serde_json::json!({"JobCreated":{"job_key":20,"element_instance_key":k(job_owner),"element_id":"a","job_type":"w","instance_key":10}}),
        ]
    };
    // Positive control: a clean remint plus a pure reorder reconciles.
    let mut reversed = tree(100, 12);
    reversed.reverse();
    nested_gate(tree(0, 12), reversed).expect("a reminted, reordered tree reconciles");
    assert!(
        nested_gate(tree(0, 12), tree(100, 14)).is_err(),
        "a job re-pointed at the OTHER execution's child must not reconcile"
    );
}

/// #1528 (review): the mint bypass for an MI child's generic re-sighting is
/// restricted to that lifecycle — a key reused for an UNRELATED entity (a
/// `UserTaskCreated` reusing its child's key, or a child reusing a job key)
/// is a collision and fails closed, never silently accepted.
#[test]
fn prev_release_gate_remint_rejects_unrelated_key_collisions() {
    let head = head_records();
    let i = mi_create_entry(&head);

    // A `UserTaskCreated` reusing its child's key: set the first
    // `UserTaskCreated`'s `user_task_key` to the child's key.
    let mut prev = head.clone();
    let child_key = head
        .iter()
        .filter(|r| r.get("state").is_none())
        .nth(i)
        .and_then(|r| r.get("events").cloned())
        .and_then(|e| e.as_array().cloned())
        .and_then(|events| {
            events.iter().find_map(|e| {
                e.get("MultiInstanceChildActivated")?
                    .get("child_key")?
                    .as_u64()
            })
        })
        .expect("a child key");
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[i].get_mut("events") {
        for e in events.iter_mut() {
            if let Some(utc) = e.get_mut("UserTaskCreated") {
                utc["user_task_key"] = serde_json::json!(child_key);
                break;
            }
        }
    }
    let err = checked(&head, &prev, mi_declaration(), &[]).unwrap_err();
    assert!(
        err.contains("minted for two entities"),
        "a UserTaskCreated reusing its child's key must fail as a collision: {err}"
    );

    // A child reusing a JOB key: set the first `MultiInstanceChildActivated`'s
    // `child_key` to a job key minted elsewhere in the corpus.
    let job_key = head
        .iter()
        .filter(|r| r.get("state").is_none())
        .filter_map(|r| r.get("events")?.as_array().cloned())
        .flatten()
        .find_map(|e| e.get("JobCreated")?.get("job_key")?.as_u64())
        .expect("a job key");
    let mut prev = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[i].get_mut("events") {
        for e in events.iter_mut() {
            if let Some(mica) = e.get_mut("MultiInstanceChildActivated") {
                mica["child_key"] = serde_json::json!(job_key);
                break;
            }
        }
    }
    let err = checked(&head, &prev, mi_declaration(), &[]).unwrap_err();
    assert!(
        err.contains("minted for two entities"),
        "a child reusing a job key must fail as a collision: {err}"
    );
}

/// #1528 (review): the mint bypass for an MI child's generic re-sighting is
/// restricted to the child's OWN `ElementActivating`/`ElementActivated` — a
/// sighting naming the SAME owning instance and element id the child was
/// registered with. An `ElementActivating` for a DIFFERENT element, or a
/// `ParallelJoinOpened` parking on the child's key, also produces an
/// `element:` base but is NOT that lifecycle re-sighting, so it must fall
/// through to `mint` and fail as the collision it is.
#[test]
fn prev_release_gate_remint_rejects_mi_child_key_reuse_by_other_elements() {
    let head = head_records();
    let i = mi_create_entry(&head);
    let child_key = head
        .iter()
        .filter(|r| r.get("state").is_none())
        .nth(i)
        .and_then(|r| r.get("events").cloned())
        .and_then(|e| e.as_array().cloned())
        .and_then(|events| {
            events.iter().find_map(|e| {
                e.get("MultiInstanceChildActivated")?
                    .get("child_key")?
                    .as_u64()
            })
        })
        .expect("a child key");

    // (1) An `ElementActivating` for a DIFFERENT element reusing the child's key.
    let mut prev = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[i].get_mut("events") {
        for e in events.iter_mut() {
            if let Some(ea) = e.get_mut("ElementActivating") {
                if ea.get("element_instance_key").and_then(|k| k.as_u64()) == Some(child_key) {
                    ea["element_id"] = serde_json::json!("approve"); // the child is `review`
                    break;
                }
            }
        }
    }
    let err = checked(&head, &prev, mi_declaration(), &[]).unwrap_err();
    assert!(
        err.contains("minted for two entities"),
        "an ElementActivating for a different element reusing the child's key must collide: {err}"
    );

    // (2) A parked `ParallelJoinOpened` reusing the child's key.
    let mut prev = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[i].get_mut("events") {
        events.push(serde_json::json!({"ParallelJoinOpened":{
            "instance_key":10,
            "element_instance_key":child_key,
            "element_id":"join",
            "scope":0
        }}));
    }
    let err = checked(&head, &prev, mi_declaration(), &[]).unwrap_err();
    assert!(
        err.contains("minted for two entities"),
        "a parked ParallelJoinOpened reusing the child's key must collide: {err}"
    );
}

/// #1528 (review): an `unbalanced` declaration must not ratify a WRONG
/// one-sided event. A surplus `UserTaskCreated` whose owning
/// `element_instance_key` its own side never minted (a wrong-child /
/// wrong-owner event) fails even under a valid declaration; and a corrupted
/// (undecodable) surplus event fails too.
#[test]
fn prev_release_gate_unbalanced_rejects_wrong_one_sided_events() {
    let head = head_records();
    let i = mi_create_entry(&head);

    // A wrong-owner surplus: point the first `UserTaskCreated`'s
    // `element_instance_key` at a key the head side never mints.
    let mut prev = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[i].get_mut("events") {
        for e in events.iter_mut() {
            if let Some(utc) = e.get_mut("UserTaskCreated") {
                utc["element_instance_key"] = serde_json::json!(9_999_999u64);
                break;
            }
        }
    }
    let err = checked(&head, &prev, mi_declaration(), &[]).unwrap_err();
    assert!(
        err.contains("never minted") || err.contains("UNDECLARED extra divergence"),
        "a wrong-owner surplus UserTaskCreated must fail: {err}"
    );

    // A corrupted surplus: mangle a `UserTaskCreated` so it no longer decodes
    // under the head `Event` type (a non-object body).
    let mut prev = head.clone();
    if let Some(serde_json::Value::Array(events)) = entries_mut(&mut prev)[i].get_mut("events") {
        for e in events.iter_mut() {
            if e.get("UserTaskCreated").is_some() {
                *e = serde_json::json!({"UserTaskCreated": "corrupted"});
                break;
            }
        }
    }
    let err = checked(&head, &prev, mi_declaration(), &[]).unwrap_err();
    assert!(
        err.contains("does not decode") || err.contains("UNDECLARED extra divergence"),
        "a corrupted surplus UserTaskCreated must fail: {err}"
    );
}

/// [`head_records`], computed once per test binary (a full corpus run).
fn cached_head_records() -> &'static [serde_json::Value] {
    static CACHE: std::sync::OnceLock<Vec<serde_json::Value>> = std::sync::OnceLock::new();
    CACHE.get_or_init(head_records)
}

/// A real `UserTaskCreated` event from the corpus (so it decodes under the
/// head `Event` type), re-keyed to `user_task_key` / `owner`.
fn user_task_created(user_task_key: u64, owner: u64, instance: u64) -> serde_json::Value {
    let mut e = cached_head_records()
        .iter()
        .filter_map(|r| r.get("events")?.as_array().cloned())
        .flatten()
        .find(|e| e.get("UserTaskCreated").is_some())
        .expect("the corpus emits UserTaskCreated");
    let body = e.get_mut("UserTaskCreated").expect("body");
    body["user_task_key"] = serde_json::json!(user_task_key);
    body["element_instance_key"] = serde_json::json!(owner);
    body["element_id"] = serde_json::json!("review");
    if body.get("instance_key").is_some() {
        body["instance_key"] = serde_json::json!(instance);
    }
    e
}

/// A self-contained MI create entry: instance 10, body `body` activating one
/// child `child`, plus `extra` events.
fn mi_entry(body: u64, child: u64, extra: Vec<serde_json::Value>) -> serde_json::Value {
    let mut events = vec![
        serde_json::json!({"ProcessInstanceCreated":{"business_id":null,"created_at":1,"instance_key":10,"parent_element_instance_key":null,"parent_process_instance_key":null,"process_definition_key":9,"process_id":"p","tags":[],"variables":{},"version":1}}),
        serde_json::json!({"ElementActivating":{"element_id":"review","element_instance_key":body,"instance_key":10,"scope":0}}),
        serde_json::json!({"ElementActivating":{"element_id":"review","element_instance_key":child,"instance_key":10,"scope":body}}),
        serde_json::json!({"MultiInstanceChildActivated":{"body_key":body,"child_key":child,"index":0,"instance_key":10,"local_variables":{}}}),
    ];
    events.extend(extra);
    serde_json::json!({"i": 0, "events": events})
}

/// An MI create entry like [`mi_entry`] but with TWO children `c1`, `c2`.
fn mi_entry2(body: u64, c1: u64, c2: u64, extra: Vec<serde_json::Value>) -> serde_json::Value {
    let mut e = mi_entry(body, c1, vec![]);
    let events = e["events"].as_array_mut().expect("events");
    events.push(serde_json::json!({"ElementActivating":{"element_id":"review","element_instance_key":c2,"instance_key":10,"scope":body}}));
    events.push(serde_json::json!({"MultiInstanceChildActivated":{"body_key":body,"child_key":c2,"index":1,"instance_key":10,"local_variables":{}}}));
    events.extend(extra);
    e
}

/// A real state checkpoint at position `i` with snapshot `format`.
fn state_at(i: u64, format: u64) -> serde_json::Value {
    let mut s = cached_head_records()
        .iter()
        .find(|r| r.get("state").is_some())
        .cloned()
        .expect("a state checkpoint");
    s["i"] = serde_json::json!(i);
    s["format"] = serde_json::json!(format);
    // The synthetic streams create instance 10 of process `p` in entry 0
    // (`mi_entry`), so — exactly as the engine's cumulative counter would —
    // every checkpoint counts that creation.
    let created = &mut s["state"]["state"]["created_by_process"]["p"];
    *created = serde_json::json!(created.as_u64().unwrap_or(0) + 1);
    s
}

/// `cross_release_checked` over a single-scenario (`s`) synthetic run.
fn checked_s(
    head: &[serde_json::Value],
    prev: &[serde_json::Value],
    expected: serde_json::Value,
) -> Result<compare::CheckedReport, String> {
    let allow = serde_json::json!({"divergences": [{
        "scenario": "s", "expected": expected,
        "introduced_in": "v0.0.2", "issue": "#1528", "upgrade": "upgrade-boundary",
        "rationale": "upgrade-boundary: test"
    }]});
    let n = head.iter().filter(|r| r.get("state").is_none()).count();
    let scenarios = vec!["s".to_string(); n];
    let allowlist = compare::parse_allowlist(&allow.to_string())?;
    let added = compare::Vocab::new();
    let ctx = compare::CrossReleaseContext {
        scenarios: &scenarios,
        allowlist: &allowlist,
        prev_tag: "v0.0.1",
        added: &added,
    };
    compare::cross_release_checked(head, prev, &ctx)
}

/// #1528 (review): an MI body activated in the SAME entry as its child, with
/// both keys differing across releases, reconciles under `remint_keys` — the
/// body's identity is registered before the child's is derived from it.
#[test]
fn prev_release_gate_same_entry_body_and_child_remint() {
    let head = vec![mi_entry(11, 12, vec![]), state_at(0, 3)];
    let prev = vec![mi_entry(21, 22, vec![]), state_at(0, 3)];
    checked_s(&head, &prev, serde_json::json!({"remint_keys": true}))
        .expect("a same-entry body+child remint reconciles");
}

/// #1528 (review): the one-sided surplus owner check runs with AND without
/// `remint_keys`, and whatever the snapshot formats — a surplus
/// `UserTaskCreated` naming an unminted owner always fails.
#[test]
fn prev_release_gate_unbalanced_owner_check_is_unconditional() {
    let bad = || user_task_created(500, 9_999_999, 10);
    let good = || user_task_created(500, 12, 10);
    let expected = |remint: bool| {
        serde_json::json!({
            "remint_keys": remint,
            "only_in_head": [{"variant": "UserTaskCreated"}],
            "unbalanced": {"reason": "a real reason"}
        })
    };
    for remint in [false, true] {
        for prev_format in [3, 2] {
            let prev = vec![mi_entry(11, 12, vec![]), state_at(0, prev_format)];
            let ok_head = vec![mi_entry(11, 12, vec![good()]), state_at(0, 3)];
            checked_s(&ok_head, &prev, expected(remint)).unwrap_or_else(|e| {
                panic!("remint={remint} format={prev_format}: a valid surplus passes: {e}")
            });
            let bad_head = vec![mi_entry(11, 12, vec![bad()]), state_at(0, 3)];
            let err = checked_s(&bad_head, &prev, expected(remint)).unwrap_err();
            assert!(
                err.contains("never minted"),
                "remint={remint} format={prev_format}: a wrong-owner surplus must fail: {err}"
            );
        }
    }
}

/// The `unbalanced` divergence of [`prev_release_gate_same_format_ratified_state_change`].
fn unbalanced_expected() -> serde_json::Value {
    serde_json::json!({
        "only_in_head": [{"variant": "UserTaskCreated"}],
        "unbalanced": {"reason": "a real reason"}
    })
}

/// Head/prev streams: a plain entry 0 + checkpoint, then the ratified
/// divergent entry 1 (head adds a task) + `head_after` / `prev_after`.
fn boundary_streams(
    head_after: serde_json::Value,
    prev_after: serde_json::Value,
) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
    boundary_streams_with(head_after, prev_after, vec![], vec![])
}

/// [`boundary_streams`] with extra events appended to each side's divergent
/// entry 1 (e.g. a `JobCreated` minting a key on each side).
fn boundary_streams_with(
    head_after: serde_json::Value,
    prev_after: serde_json::Value,
    head_extra: Vec<serde_json::Value>,
    prev_extra: Vec<serde_json::Value>,
) -> (Vec<serde_json::Value>, Vec<serde_json::Value>) {
    let mut head_events = vec![user_task_created(500, 32, 10)];
    head_events.extend(head_extra);
    let at = |mut e: serde_json::Value, i: u64| {
        e["i"] = serde_json::json!(i);
        e
    };
    let head = vec![
        at(mi_entry(11, 12, vec![]), 0),
        state_at(0, 3),
        at(mi_entry(31, 32, head_events), 1),
        head_after,
    ];
    let prev = vec![
        at(mi_entry(11, 12, vec![]), 0),
        state_at(0, 3),
        at(mi_entry(31, 32, prev_extra), 1),
        prev_after,
    ];
    (head, prev)
}

/// #1528 (review): with the SAME snapshot format on both sides, checkpoints
/// past a ratified upgrade-boundary `unbalanced` divergence are compared
/// key-insensitively with the ratified instances excluded — a key shift
/// passes, but an UNRELATED state change, an undecodable checkpoint, or a
/// difference before the divergence still fails.
#[test]
fn prev_release_gate_same_format_ratified_state_change() {
    // A shifted key allocator (every later key moves) is not a regression.
    let mut shifted = state_at(1, 3);
    shifted["state"]["next_local"] = serde_json::json!(987_654_321u64);
    let (head, prev) = boundary_streams(shifted, state_at(1, 3));
    let report = checked_s(&head, &prev, unbalanced_expected())
        .expect("a key shift after the divergent entry passes");
    assert_eq!(report.base.states_compared, 1);
    assert_eq!(report.base.states_compared_outside_ratified_boundary, 1);

    // An unrelated post-boundary state change is still caught.
    let mut unrelated = state_at(1, 3);
    let flag = unrelated["state"]["state"]["lenient_completion"]
        .as_bool()
        .expect("lenient_completion in the snapshot");
    unrelated["state"]["state"]["lenient_completion"] = serde_json::json!(!flag);
    let (head, prev) = boundary_streams(unrelated, state_at(1, 3));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(err.contains("outside the ratified"), "{err}");

    // A malformed post-boundary checkpoint still fails to decode.
    let mut malformed = state_at(1, 3);
    malformed["state"] = serde_json::Value::Null;
    let (head, prev) = boundary_streams(malformed, state_at(1, 3));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(err.contains("does not decode"), "{err}");

    // A state difference BEFORE the divergent entry is compared strictly.
    let (mut head, prev) = boundary_streams(state_at(1, 3), state_at(1, 3));
    head[1]["state"]["next_local"] = serde_json::json!(987_654_321u64);
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(err.contains("durable state diverges at entry 0"), "{err}");
}

/// #1528 (review): an `unbalanced` marker is validated even when the matched
/// events balance, and a balanced scenario never enables the boundary
/// projection (its later checkpoints are still compared strictly).
#[test]
fn prev_release_gate_unbalanced_marker_on_balanced_pattern() {
    let head = vec![mi_entry(11, 12, vec![]), state_at(0, 3)];
    let prev = vec![mi_entry(21, 22, vec![]), state_at(0, 3)];
    let err = checked_s(
        &head,
        &prev,
        serde_json::json!({"remint_keys": true, "unbalanced": {"reason": ""}}),
    )
    .unwrap_err();
    assert!(err.contains("non-empty reason"), "{err}");

    let mut shifted = state_at(0, 3);
    shifted["state"]["next_local"] = serde_json::json!(987_654_321u64);
    let head = vec![mi_entry(11, 12, vec![]), shifted];
    let err = checked_s(
        &head,
        &prev,
        serde_json::json!({"remint_keys": true, "unbalanced": {"reason": "real"}}),
    )
    .unwrap_err();
    assert!(
        err.contains("durable state diverges at entry 0 (same"),
        "a balanced scenario compares strictly: {err}"
    );
}

/// #1528 (review): the surplus `UserTaskCreated` must be exactly one per
/// activated MI child — a duplicate owner, a task on another valid (minted)
/// element instance, or a child without its task all fail.
#[test]
fn prev_release_gate_unbalanced_one_task_per_child() {
    let expected = serde_json::json!({
        "only_in_head": [{"variant": "UserTaskCreated"}],
        "unbalanced": {"reason": "a real reason"}
    });
    let prev = vec![mi_entry2(11, 12, 13, vec![]), state_at(0, 2)];
    let run = |tasks: Vec<serde_json::Value>| {
        checked_s(
            &[mi_entry2(11, 12, 13, tasks), state_at(0, 3)],
            &prev,
            expected.clone(),
        )
    };
    run(vec![
        user_task_created(500, 12, 10),
        user_task_created(501, 13, 10),
    ])
    .expect("one task per child passes");
    for (case, tasks) in [
        (
            "duplicate owner",
            vec![
                user_task_created(500, 12, 10),
                user_task_created(501, 12, 10),
            ],
        ),
        (
            "wrong valid owner",
            vec![
                user_task_created(500, 12, 10),
                user_task_created(501, 11, 10),
            ],
        ),
        ("missing child", vec![user_task_created(500, 12, 10)]),
    ] {
        let err = run(tasks).unwrap_err();
        assert!(
            err.contains("one task per") || err.contains("share the owner"),
            "{case} must fail: {err}"
        );
    }
}

/// #1528 (review): the boundary projection must be schema-aware, not
/// shape-heuristic. (a) A numeric-keyed map that is NOT a schema entity map
/// (an instance's `scope_parents`) keeps its key→value associations, so
/// swapping two of its values in an UNRELATED instance fails. (b) A numeric
/// business-variable NAME is data, not a key: renaming `"1"` to `"2"` fails.
/// (c) A nested FEEL context entry named `next_local` is data: only the
/// SNAPSHOT ROOT's key allocator is dropped, so a changed nested
/// `next_local` fails.
#[test]
fn prev_release_gate_boundary_projection_is_schema_aware() {
    let unrelated = |mutate: &dyn Fn(&mut serde_json::Value)| {
        let mut s = state_at(1, 3);
        let instances = s["state"]["state"]["instances"]
            .as_object_mut()
            .expect("instances map");
        let key = instances
            .keys()
            .find(|k| k.parse::<u64>().ok() != Some(10))
            .cloned()
            .expect("an instance other than the ratified 10");
        mutate(&mut instances[&key]);
        s
    };

    // (a) swap two scope-parent values inside an unrelated instance.
    let swapped = unrelated(&|inst| {
        let parents = inst["scope_parents"]
            .as_object_mut()
            .expect("scope_parents");
        parents.insert("91".to_owned(), serde_json::json!(92u64));
        parents.insert("92".to_owned(), serde_json::json!(91u64));
        let tmp = parents["91"].clone();
        *parents.get_mut("91").unwrap() = parents["92"].clone();
        *parents.get_mut("92").unwrap() = tmp;
    });
    let (head, prev) = boundary_streams(swapped, state_at(1, 3));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("outside the ratified"),
        "a swapped scope_parents entry in an unrelated instance must fail: {err}"
    );

    // (b) rename a numeric business-variable name ("1" -> "2").
    let renamed = unrelated(&|inst| {
        let vars = inst["variables"].as_object_mut().expect("variables");
        vars.insert("1".to_owned(), serde_json::json!({"Int": 7}));
        let value = vars.remove("1").unwrap();
        vars.insert("2".to_owned(), value);
    });
    let (head, prev) = boundary_streams(renamed, state_at(1, 3));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("outside the ratified"),
        "renaming a numeric business-variable name must fail: {err}"
    );

    // (c) a nested FEEL context entry named `next_local` is data.
    let nested = unrelated(&|inst| {
        let vars = inst["variables"].as_object_mut().expect("variables");
        vars.insert(
            "ctx".to_owned(),
            serde_json::json!({"Map": {"next_local": {"Int": 1}}}),
        );
        vars["ctx"]["Map"]["next_local"] = serde_json::json!({"Int": 2});
    });
    let (head, prev) = boundary_streams(nested, state_at(1, 3));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("outside the ratified"),
        "a nested `next_local` data entry must not be erased: {err}"
    );

    // Sanity: the ROOT allocator difference still passes (it is the one
    // field the projection drops).
    let mut shifted = state_at(1, 3);
    shifted["state"]["next_local"] = serde_json::json!(987_654_321u64);
    let (head, prev) = boundary_streams(shifted, state_at(1, 3));
    checked_s(&head, &prev, unbalanced_expected())
        .expect("the root key allocator difference still passes");
}

/// #1528 (review): the boundary projection canonicalizes key REFERENCES to
/// their entity identities, not to one common placeholder — an unrelated
/// retained job whose `element_instance_key` points at the WRONG active
/// element must still fail after the boundary.
#[test]
fn prev_release_gate_boundary_projection_preserves_reference_identity() {
    let mut wrong_owner = state_at(1, 3);
    let elements: Vec<u64> = wrong_owner["state"]["state"]["instances"]
        .as_object()
        .expect("instances")
        .values()
        .filter(|i| i["key"].as_u64() != Some(10))
        .flat_map(|i| {
            i["active"].as_object().into_iter().flat_map(|m| {
                m.keys()
                    .filter_map(|k| k.parse::<u64>().ok())
                    .collect::<Vec<_>>()
            })
        })
        .collect();
    let job = wrong_owner["state"]["state"]["jobs"]
        .as_object_mut()
        .expect("jobs map")
        .values_mut()
        .find(|j| j["instance_key"].as_u64() != Some(10))
        .expect("a retained job of an unrelated instance");
    let wrong = elements
        .iter()
        .copied()
        .find(|e| Some(*e) != job["element_instance_key"].as_u64())
        .expect("a second active element to mis-point at");
    job["element_instance_key"] = serde_json::json!(wrong);
    let (head, prev) = boundary_streams(wrong_owner, state_at(1, 3));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("outside the ratified"),
        "a mis-owned retained job after the boundary must fail: {err}"
    );
}

/// The post-boundary checkpoint of the ratified-instance scenario: the
/// ratified instance 10 (process `p`) is added in flight,
/// and the retained job `33` of the UNRELATED instance 30 is stored under
/// `job_key` (a key reminted on one side only must reconcile through that
/// side's own `KeyIds`, #1764).
fn with_ratified(job_key: u64) -> serde_json::Value {
    let mut s = state_at(1, 3);
    let state = s["state"]["state"].as_object_mut().expect("state");
    let mut inst = state["instances"]
        .as_object()
        .expect("instances")
        .values()
        .next()
        .expect("a template instance")
        .clone();
    inst["key"] = serde_json::json!(10u64);
    inst["process_id"] = serde_json::json!("p");
    inst["state"] = serde_json::json!("Active");
    inst["terminated_epoch"] = serde_json::json!(0u64);
    state["instances"]
        .as_object_mut()
        .expect("instances")
        .insert("10".to_owned(), inst);
    // Its creation is already counted by `state_at`; it is in flight.
    let n = state["inflight_by_process"]["p"].as_u64().unwrap_or(0);
    state["inflight_by_process"]["p"] = serde_json::json!(n + 1);
    let jobs = state["jobs"].as_object_mut().expect("jobs");
    let mut job = jobs.remove("33").expect("job 33");
    job["key"] = serde_json::json!(job_key);
    jobs.insert(job_key.to_string(), job);
    let set = state["jobs_by_instance"]["30"]
        .as_array_mut()
        .expect("jobs_by_instance[30]");
    assert_eq!(set.len(), 1, "one job under instance 30");
    set[0] = serde_json::json!(job_key);
    let slot = state["activatable_jobs"]["payment"]
        .as_array_mut()
        .expect("activatable_jobs[payment]");
    let pos = slot
        .iter()
        .position(|e| e[1].as_u64() == Some(33))
        .expect("job 33 in the activatable index");
    slot[pos][1] = serde_json::json!(job_key);
    s
}

/// The prev behaviour completes the ratified instance at the boundary: it
/// turns terminal, its process's in-flight count drops, `terminal_epoch`
/// advances and the instance joins `recently_terminated`.
fn completed_ratified(job_key: u64) -> serde_json::Value {
    let mut s = with_ratified(job_key);
    let state = s["state"]["state"].as_object_mut().expect("state");
    state["instances"]["10"]["state"] = serde_json::json!("Completed");
    let n = state["inflight_by_process"]["p"].as_u64().expect("p count");
    if n == 1 {
        state["inflight_by_process"]
            .as_object_mut()
            .expect("counts")
            .remove("p");
    } else {
        state["inflight_by_process"]["p"] = serde_json::json!(n - 1);
    }
    let epoch = state["terminal_epoch"].as_u64().expect("terminal_epoch");
    state["terminal_epoch"] = serde_json::json!(epoch + 1);
    state["instances"]["10"]["terminated_epoch"] = serde_json::json!(epoch + 1);
    state["recently_terminated"]
        .as_array_mut()
        .expect("recently_terminated")
        .push(serde_json::json!(10u64));
    s
}

/// A `JobCreated` minting `job_key` for the MI child 32 of instance 10.
fn job_created(job_key: u64) -> serde_json::Value {
    job_created_for(job_key, 10)
}

/// A `JobCreated` minting `job_key` for instance `owner`.
fn job_created_for(job_key: u64, owner: u64) -> serde_json::Value {
    serde_json::json!({"JobCreated": {"created_at": 1, "element_id": "review",
        "element_instance_key": 32, "instance_key": owner, "job_key": job_key,
        "job_type": "payment", "priority": 50, "retries": 3}})
}

/// The ratified scenario with each side's job mint declared re-mintable.
fn remint_unbalanced_expected() -> serde_json::Value {
    let mut e = unbalanced_expected();
    e["remint_keys"] = serde_json::json!(true);
    e
}

/// #1528/#1764: excluding the ratified instance must also normalize its
/// DERIVED contributions (`inflight_by_process`, `created_by_process`,
/// `terminal_epoch`, `recently_terminated`): a checkpoint from each side's
/// real behaviour compares equal through the projection, while an unrelated
/// derived-counter change still fails.
#[test]
fn prev_release_gate_boundary_projection_normalizes_derived_state() {
    let (head, prev) = boundary_streams(with_ratified(33), completed_ratified(33));
    checked_s(&head, &prev, unbalanced_expected()).unwrap_or_else(|e| {
        panic!("the declared completion of the ratified instance reconciles: {e}")
    });

    // An UNRELATED derived-counter change (another process's count) fails.
    let mut unrelated = with_ratified(33);
    let counts = unrelated["state"]["state"]["inflight_by_process"]
        .as_object_mut()
        .expect("inflight_by_process");
    let (other, base) = counts
        .iter()
        .find(|(k, _)| **k != "p")
        .map(|(k, v)| (k.clone(), v.as_u64().expect("count")))
        .expect("a second process count");
    counts[&other] = serde_json::json!(base + 1);
    let (head, prev) = boundary_streams(unrelated, with_ratified(33));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("outside the ratified"),
        "an unrelated inflight count change must fail: {err}"
    );
}

/// #1764: only the ratified instance's OWN contribution to its process's
/// counts is subtracted — a wrong count contributed by ANOTHER instance of
/// the SAME process still fails (the whole process entry is never dropped).
#[test]
fn prev_release_gate_boundary_projection_counts_scoped_to_ratified_instance() {
    for counts in ["inflight_by_process", "created_by_process"] {
        let mut wrong = with_ratified(33);
        let n = wrong["state"]["state"][counts]["p"]
            .as_u64()
            .expect("p count");
        wrong["state"]["state"][counts]["p"] = serde_json::json!(n + 1);
        let (head, prev) = boundary_streams(wrong, completed_ratified(33));
        let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
        assert!(
            err.contains("outside the ratified"),
            "a second `p` instance's {counts} change must fail: {err}"
        );
    }
}

/// #1764: a raw-key difference on a retained job WITHOUT a mint explaining
/// it fails. (A remint is only ever observed in a divergent entry, whose
/// touched instances are ratified — so a reminted job's owner is excluded,
/// never compared; per-side `KeyIds` canonicalization of RETAINED keys is
/// exercised by `prev_release_gate_boundary_projection_remints_nested_instance_keys`.)
#[test]
fn prev_release_gate_boundary_projection_canonicalizes_through_key_ids() {
    let (head, prev) = boundary_streams(with_ratified(133), completed_ratified(33));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("outside the ratified"),
        "an unexplained raw-key remint must fail: {err}"
    );
}

/// #1764: `jobs_by_instance` keeps its owner→jobs association — swapping
/// two unrelated owners' job sets fails (a sorted multiset would hide it).
#[test]
fn prev_release_gate_boundary_projection_preserves_reverse_index_owners() {
    let mut swapped = with_ratified(33);
    let index = swapped["state"]["state"]["jobs_by_instance"]
        .as_object_mut()
        .expect("jobs_by_instance");
    let owners: Vec<String> = index
        .iter()
        .filter(|(k, v)| *k != "10" && v.as_array().is_some_and(|a| !a.is_empty()))
        .map(|(k, _)| k.clone())
        .collect();
    let (a, b) = match owners.as_slice() {
        [a, b, ..] => (a.clone(), b.clone()),
        _ => {
            // Only one owner: re-point its set at a second (job-less) owner.
            let a = owners.first().cloned().expect("an owner with jobs");
            (a, "30000".to_owned())
        }
    };
    let va = index.remove(&a).unwrap_or(serde_json::json!([]));
    let vb = index.remove(&b).unwrap_or(serde_json::json!([]));
    index.insert(a, vb);
    index.insert(b, va);
    let (head, prev) = boundary_streams(swapped, with_ratified(33));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("outside the ratified"),
        "swapping jobs_by_instance owners must fail: {err}"
    );
}

/// #1764: the State rules apply ONLY at their typed path — a business
/// variable named like a State field (`jobs`, `terminal_epoch`,
/// `inflight_by_process`) is data, so changing its value fails.
#[test]
fn prev_release_gate_boundary_projection_rules_are_path_anchored() {
    for name in [
        "jobs",
        "terminal_epoch",
        "inflight_by_process",
        "next_local",
    ] {
        let set = |v: i64| {
            let mut s = with_ratified(33);
            let inst = s["state"]["state"]["instances"]
                .as_object_mut()
                .expect("instances")
                .iter_mut()
                .find(|(k, _)| *k != "10")
                .map(|(_, v)| v)
                .expect("an unrelated instance");
            inst["variables"]
                .as_object_mut()
                .expect("variables")
                .insert(name.to_owned(), serde_json::json!({"Int": v}));
            s
        };
        let (head, prev) = boundary_streams(set(1), set(2));
        let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
        assert!(
            err.contains("outside the ratified"),
            "a variable named `{name}` is data and must be compared: {err}"
        );
    }
}

/// #1528 (review): the one-task-per-child validation must run even when the
/// side emits NO surplus `UserTaskCreated` at all — every activated MI child
/// of an element the declaration's `only_in_*` patterns name must carry its
/// task, so an all-tasks-missing stream fails.
#[test]
fn prev_release_gate_unbalanced_all_tasks_missing_fails() {
    let expected = serde_json::json!({
        "reorder": true,
        "only_in_head": [
            {"variant": "UserTaskCreated", "element_id": "review"},
            {"variant": "VariablesUpdated"}
        ],
        "unbalanced": {"reason": "a real reason"}
    });
    let prev = vec![mi_entry2(11, 12, 13, vec![]), state_at(0, 2)];
    // Head activates the same two children but emits NO UserTaskCreated. It
    // adds a declared `VariablesUpdated` so the entry diverges (the declared
    // patterns are unbalanced) and the scenario reaches the surplus
    // validation; the all-tasks-missing check is what must fail.
    let head_entry = mi_entry2(
        11,
        12,
        13,
        vec![
            serde_json::json!({"VariablesUpdated":{"instance_key":10,"scope":11,"variables":{"x":{"Int":1}}}}),
        ],
    );
    let head = vec![head_entry, state_at(0, 3)];
    let err = checked_s(&head, &prev, expected).unwrap_err();
    assert!(
        err.contains("one task per activated child"),
        "an all-tasks-missing surplus must fail: {err}"
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

/// #1528 (review): the MI BODY's generic-activation mint bypass covers only
/// the body's OWN activation (same instance, same element). An activation of
/// a DIFFERENT element reusing the body's key, or a job reusing it, is a
/// collision and fails closed.
#[test]
fn prev_release_gate_rejects_mi_body_key_reuse() {
    let other_element = |shift: u64| {
        let mut t = nested_tree(shift);
        t.push(serde_json::json!({"ElementActivating":{"element_id":"zzz","element_instance_key":11 + shift,"instance_key":10,"scope":0}}));
        t
    };
    let err = nested_gate(other_element(0), other_element(100)).unwrap_err();
    assert!(err.contains("minted for two entities"), "{err}");

    let job_first = |shift: u64| {
        let mut t = nested_tree(shift);
        t.insert(1, serde_json::json!({"JobCreated":{"job_key":11 + shift,"element_instance_key":12 + shift,"element_id":"outer","job_type":"w","instance_key":10}}));
        t
    };
    let err = nested_gate(job_first(0), job_first(100)).unwrap_err();
    assert!(err.contains("minted for two entities"), "{err}");
}

/// #1528 (review): a permitted reorder may put a historical scope-less
/// `ElementActivating` AFTER the body's `ElementActivated` (which carries the
/// real parent scope). The authoritative scope must win regardless of order,
/// so a mixed-version nested stream still reconciles under reorder + remint.
#[test]
fn prev_release_gate_nested_mi_body_scope_is_order_independent() {
    let mixed = |shift: u64| {
        let mut t = nested_tree(shift);
        // Inner body 14 (scope 12): add its ElementActivated (authoritative
        // scope) and move a scope-less historical ElementActivating after it.
        let pos = t
            .iter()
            .position(|e| e["ElementActivating"]["element_instance_key"] == 14 + shift)
            .unwrap();
        t[pos] = serde_json::json!({"ElementActivated":{"element_id":"inner","element_instance_key":14 + shift,"instance_key":10,"scope":12 + shift}});
        t.insert(pos + 1, serde_json::json!({"ElementActivating":{"element_id":"inner","element_instance_key":14 + shift,"instance_key":10}}));
        t
    };
    let mut reordered = mixed(100);
    // Same tree on the other side, with the scope-less record FIRST.
    let pos = reordered
        .iter()
        .position(|e| e["ElementActivated"]["element_instance_key"] == 114)
        .unwrap();
    reordered.swap(pos, pos + 1);
    nested_gate(mixed(0), reordered).expect("the authoritative scope wins regardless of order");
}
/// #1764 guard: every serialized field of `State` and `EngineSnapshot` is
/// classified by exactly one projection rule — adding a field without
/// classifying it fails here (and fails the gate closed at runtime).
#[test]
fn state_projection_classifies_every_state_field() {
    compare::projection_rule_drift().expect("projection rules match the schema");
    let fields =
        compare::struct_fields_in(include_str!("../src/state/types.rs"), "State").expect("State");
    assert!(fields.contains("lenient_completion") && fields.contains("instances"));
    assert!(
        !fields.contains("deferred_jobs"),
        "a #[serde(skip)] field is never serialized, so never classified"
    );
    // The derivation sees a newly declared field.
    let src = "pub struct State {\n    pub a: u64,\n    #[cfg_attr(feature = \"serde\", serde(skip))]\n    pub b: BTreeMap<Key, u64>,\n    /// doc <\n    pub c: Option<(u64, String)>,\n}\n";
    assert_eq!(
        compare::struct_fields_in(src, "State").expect("parsed"),
        ["a", "c"]
            .into_iter()
            .map(str::to_owned)
            .collect::<BTreeSet<_>>()
    );
}

/// An `ElementActivating` minting element instance `key` (element `x`). Like
/// [`job_created`] it rides the divergent entry's instance 10 — an event
/// naming instance 30 would make the boundary exclude 30 wholesale — so the
/// mint's identity is what instance 30's nested containers reference.
fn element_activating(key: u64) -> serde_json::Value {
    serde_json::json!({"ElementActivating": {"element_id": "x",
        "element_instance_key": key, "instance_key": 10, "scope": 0}})
}

/// #1764 (review): the engine keys NESTED in a retained instance
/// (`active`, `scopes`, `scope_parents`, `activating`, `parked_joins`,
/// `join_instances`) canonicalize through each side's own `KeyIds`, so a
/// one-sided remint reconciles — while the same raw-key difference with no
/// mint explaining it, or a swapped association, still fails.
#[test]
fn prev_release_gate_boundary_projection_remints_nested_instance_keys() {
    let with_element = |raw: u64| {
        let mut s = with_ratified(33);
        let inst = s["state"]["state"]["instances"]["30"]
            .as_object_mut()
            .expect("retained instance 30");
        inst.insert(
            "active".to_owned(),
            serde_json::json!({ raw.to_string(): "x" }),
        );
        inst.insert(
            "scopes".to_owned(),
            serde_json::json!({ raw.to_string(): raw }),
        );
        inst.insert(
            "scope_parents".to_owned(),
            serde_json::json!({ raw.to_string(): 30u64 }),
        );
        inst.insert("activating".to_owned(), serde_json::json!([raw]));
        inst.insert("parked_joins".to_owned(), serde_json::json!([raw]));
        inst.insert(
            "join_instances".to_owned(),
            serde_json::json!([{"scope": raw, "element_id": "j", "value": raw}]),
        );
        s
    };
    let (head, prev) = boundary_streams_with(
        with_element(540),
        with_element(440),
        vec![element_activating(540)],
        vec![element_activating(440)],
    );
    checked_s(&head, &prev, remint_unbalanced_expected())
        .unwrap_or_else(|e| panic!("a nested retained-instance remint reconciles: {e}"));

    let (head, prev) = boundary_streams(with_element(540), with_element(440));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("outside the ratified"),
        "an unexplained nested remint must fail: {err}"
    );
}

/// #1764 (review): `activated_jobs` is a `BTreeSet<Key>` of RETAINED jobs
/// (no mint touches any instance but the ratified 10): a permuted
/// serialization compares equal as a set, while a different member fails.
#[test]
fn prev_release_gate_boundary_projection_activated_jobs_is_a_set() {
    let activated = |keys: serde_json::Value| {
        let mut s = with_ratified(33);
        s["state"]["state"]["activated_jobs"] = keys;
        s
    };
    let (head, prev) = boundary_streams(
        activated(serde_json::json!([134, 133])),
        activated(serde_json::json!([133, 134])),
    );
    checked_s(&head, &prev, unbalanced_expected())
        .unwrap_or_else(|e| panic!("a permuted activated-jobs set reconciles: {e}"));

    let (head, prev) = boundary_streams(
        activated(serde_json::json!([133, 134])),
        activated(serde_json::json!([133, 135])),
    );
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("outside the ratified"),
        "a different retained activated member must fail: {err}"
    );
}

/// #1764 (review): a resident record naming a RETAINED owner for a job
/// this side's creation history attributes to the ratified instance is an
/// ownership mismatch, not a key to exclude.
#[test]
fn prev_release_gate_boundary_projection_rejects_resident_ownership_mismatch() {
    let (head, prev) = boundary_streams_with(
        with_ratified(133),
        completed_ratified(33),
        vec![job_created(133)],
        vec![job_created(33)],
    );
    let err = checked_s(&head, &prev, remint_unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("ownership mismatch"),
        "a history-vs-resident owner disagreement must fail: {err}"
    );
}

/// #1764 (review): the ratified instance's terminal transition shifts every
/// LATER terminal stamp by one on its side only — a subsequent identical
/// completion of an unrelated instance reconciles, a genuinely different
/// stamp still fails.
#[test]
fn prev_release_gate_boundary_projection_rebases_terminal_stamps() {
    let later = |mut s: serde_json::Value, shift: i64| {
        let state = s["state"]["state"].as_object_mut().expect("state");
        let epoch = state["terminal_epoch"].as_u64().expect("terminal_epoch") + 1;
        state["terminal_epoch"] = serde_json::json!(epoch);
        state["instances"]["30"]["terminated_epoch"] =
            serde_json::json!(epoch.saturating_add_signed(shift));
        state["recently_terminated"]
            .as_array_mut()
            .expect("recently_terminated")
            .push(serde_json::json!(30u64));
        s
    };
    let (head, prev) = boundary_streams(
        later(with_ratified(33), 0),
        later(completed_ratified(33), 0),
    );
    checked_s(&head, &prev, unbalanced_expected())
        .unwrap_or_else(|e| panic!("a later unrelated completion reconciles: {e}"));

    let (head, prev) = boundary_streams(
        later(with_ratified(33), 0),
        later(completed_ratified(33), -1),
    );
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("outside the ratified"),
        "a wrong unrelated terminal stamp must fail: {err}"
    );
}

/// #1764 (review): a job-type bucket emptied ONLY because its last job
/// belongs to the ratified instance is omitted (the engine drops an emptied
/// bucket), matching the side where that job already completed.
#[test]
fn prev_release_gate_boundary_projection_omits_buckets_emptied_by_exclusion() {
    let mut pending = with_ratified(33);
    let state = pending["state"]["state"].as_object_mut().expect("state");
    let mut job = state["jobs"]["33"].clone();
    job["key"] = serde_json::json!(77u64);
    job["instance_key"] = serde_json::json!(10u64);
    job["job_type"] = serde_json::json!("ratified-only");
    state["jobs"]
        .as_object_mut()
        .expect("jobs")
        .insert("77".to_owned(), job);
    state["activatable_jobs"]["ratified-only"] = serde_json::json!([[50, 77]]);
    let (head, prev) = boundary_streams(pending, completed_ratified(33));
    checked_s(&head, &prev, unbalanced_expected())
        .unwrap_or_else(|e| panic!("an excluded-only bucket is omitted: {e}"));

    // An already-empty bucket is still compared.
    let mut empty = with_ratified(33);
    empty["state"]["state"]["activatable_jobs"]["idle"] = serde_json::json!([]);
    let (head, prev) = boundary_streams(empty, completed_ratified(33));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(err.contains("outside the ratified"), "{err}");
}

/// #1764 (review): the `recently_terminated` ring is bounded — when the
/// ratified instance's one-sided completion lands on a FULL ring it also
/// evicts the oldest unrelated key on that side. The common covered window is
/// compared (identity and order kept), so the eviction reconciles while a
/// wrong key inside the window still fails.
#[test]
fn prev_release_gate_boundary_projection_ring_eviction_at_capacity() {
    let cap: u64 = include_str!("../src/state/types.rs")
        .lines()
        .find_map(|l| {
            l.trim()
                .strip_prefix("pub const RECENTLY_TERMINATED_CAP: usize = ")
                .and_then(|r| r.strip_suffix(';'))
                .and_then(|n| n.replace('_', "").parse().ok())
        })
        .expect("RECENTLY_TERMINATED_CAP");
    let ring = |mut s: serde_json::Value, keys: Vec<u64>| {
        s["state"]["state"]["recently_terminated"] = serde_json::json!(keys);
        s
    };
    let full: Vec<u64> = (100_000..100_000 + cap).collect();
    let mut evicted: Vec<u64> = full[1..].to_vec();
    evicted.push(10);
    let (head, prev) = boundary_streams(
        ring(with_ratified(33), full.clone()),
        ring(completed_ratified(33), evicted.clone()),
    );
    checked_s(&head, &prev, unbalanced_expected())
        .unwrap_or_else(|e| panic!("an eviction by the ratified completion reconciles: {e}"));

    let mut wrong = evicted;
    wrong.swap(3, 4);
    let (head, prev) = boundary_streams(
        ring(with_ratified(33), full),
        ring(completed_ratified(33), wrong),
    );
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(err.contains("outside the ratified"), "{err}");
}

/// #1764 (review): engine keys nested INSIDE a retained instance's
/// multi-instance / ad-hoc / compensation payloads (`active`,
/// `child_indices`, `pending_scope_subscriptions`, …) canonicalize through
/// each side's `KeyIds`, so a remint of both body and child keys
/// reconciles, while an unexplained raw difference still fails.
#[test]
fn prev_release_gate_boundary_projection_remints_nested_payload_keys() {
    let with_payloads = |body: u64, child: u64| {
        let mut s = with_ratified(33);
        let inst = s["state"]["state"]["instances"]["30"]
            .as_object_mut()
            .expect("retained instance 30");
        inst.insert(
            "multi_instances".to_owned(),
            serde_json::json!({ body.to_string(): {"element_id": "x", "sequential": false, "items": [],
                "output_collection": null, "output_element": null,
                "completion_condition": null, "input_element": null, "spawned": 1,
                "active": [child], "child_indices": { child.to_string(): 0 },
                "output_values": []}}),
        );
        inst.insert(
            "adhoc_instances".to_owned(),
            serde_json::json!({ body.to_string(): {"element_id": "x", "output_collection": null,
                "output_element": null, "iterations": 1, "active": [child]}}),
        );
        inst.insert(
            "compensation_waits".to_owned(),
            serde_json::json!({ body.to_string(): {"throw_element_id": "t", "scope": body,
                "pending_handlers": ["h"], "pending_handler_keys": [child],
                "pending_scope_subscriptions": [child], "pending_handler_scopes": [body],
                "pending_scope_subscription_parents": [body]}}),
        );
        s
    };
    let (head, prev) = boundary_streams_with(
        with_payloads(540, 541),
        with_payloads(440, 441),
        vec![element_activating(540), element_activating(541)],
        vec![element_activating(440), element_activating(441)],
    );
    checked_s(&head, &prev, remint_unbalanced_expected())
        .unwrap_or_else(|e| panic!("a nested payload remint reconciles: {e}"));

    let (head, prev) = boundary_streams(with_payloads(540, 541), with_payloads(440, 441));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(err.contains("outside the ratified"), "{err}");
}

/// #1764 (review): the ratified instance's creation is attributed from each
/// side's own `ProcessInstanceCreated`, so it is subtracted even once the
/// instance is EVICTED from `instances`, and from its CREATING process even
/// after a migration changed its current process id.
#[test]
fn prev_release_gate_boundary_projection_creations_survive_eviction_and_migration() {
    let mut evicted = completed_ratified(33);
    evicted["state"]["state"]["instances"]
        .as_object_mut()
        .expect("instances")
        .remove("10");
    let (head, prev) = boundary_streams(with_ratified(33), evicted);
    checked_s(&head, &prev, unbalanced_expected())
        .unwrap_or_else(|e| panic!("an evicted ratified instance reconciles: {e}"));

    let mut migrated = completed_ratified(33);
    migrated["state"]["state"]["instances"]["10"]["process_id"] = serde_json::json!("q");
    let (head, prev) = boundary_streams(with_ratified(33), migrated);
    checked_s(&head, &prev, unbalanced_expected())
        .unwrap_or_else(|e| panic!("a migrated ratified instance reconciles: {e}"));
}

/// #1764 (review): only a count entry EMPTIED by the exclusion is dropped —
/// a pre-existing zero entry for an unrelated process is still compared.
#[test]
fn prev_release_gate_boundary_projection_keeps_preexisting_zero_counts() {
    for counts in ["inflight_by_process", "created_by_process"] {
        let mut extra = with_ratified(33);
        extra["state"]["state"][counts]["unrelated"] = serde_json::json!(0u64);
        let (head, prev) = boundary_streams(extra, completed_ratified(33));
        let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
        assert!(
            err.contains("outside the ratified"),
            "a pre-existing zero {counts} entry must be compared: {err}"
        );
    }
}

/// #1764 (review): subtracting the ratified creations fails CLOSED on
/// underflow — a per-process count below the ratified contribution it must
/// contain is a real divergence, never normalized to an omitted entry.
#[test]
fn prev_release_gate_boundary_projection_rejects_count_underflow() {
    let zero = |mut s: serde_json::Value| {
        s["state"]["state"]["created_by_process"]["p"] = serde_json::json!(0u64);
        s
    };
    let (head, prev) = boundary_streams(zero(with_ratified(33)), zero(completed_ratified(33)));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(
        err.contains("below"),
        "an underflowing count must fail: {err}"
    );
}

fn completed(instance_key: u64) -> serde_json::Value {
    serde_json::json!({"ProcessInstanceCompleted": {"instance_key": instance_key}})
}

fn terminated(instance_key: u64) -> serde_json::Value {
    serde_json::json!({"ProcessInstanceTerminated": {"instance_key": instance_key}})
}

fn settled(s: &mut serde_json::Value, entries: &[u64]) {
    let ledger: serde_json::Map<String, serde_json::Value> = entries
        .iter()
        .map(|k| {
            (
                k.to_string(),
                serde_json::json!([{"lease_token": "t", "outcome": "completed"}]),
            )
        })
        .collect();
    s["state"]["state"]["settled_jobs"] = serde_json::Value::Object(ledger);
}

/// #1764 (review): a settled job of the ratified instance is attributed from
/// the side's own `JobCreated` (a settlement carries no owner, and eviction
/// drops the job itself), and a `settled_jobs` ledger EMPTIED by the
/// exclusion is omitted, as the engine omits an empty ledger.
#[test]
fn prev_release_gate_boundary_projection_settled_jobs_of_ratified_instance() {
    // (a) the exclusion empties the ledger.
    let mut head_after = with_ratified(33);
    settled(&mut head_after, &[77]);
    let (head, prev) = boundary_streams_with(
        head_after,
        completed_ratified(33),
        vec![job_created(77)],
        vec![job_created(77)],
    );
    checked_s(&head, &prev, unbalanced_expected())
        .unwrap_or_else(|e| panic!("an emptied settled_jobs ledger reconciles: {e}"));

    // (b) the evicted instance's settled job beside an unrelated one.
    let mut head_after = with_ratified(33);
    settled(&mut head_after, &[77, 33]);
    let mut prev_after = completed_ratified(33);
    settled(&mut prev_after, &[33]);
    let (head, prev) = boundary_streams_with(
        head_after,
        prev_after,
        vec![job_created(77)],
        vec![job_created(77)],
    );
    checked_s(&head, &prev, unbalanced_expected())
        .unwrap_or_else(|e| panic!("a ratified settled job reconciles: {e}"));

    // An unrelated settled-job difference still fails.
    let mut head_after = with_ratified(33);
    settled(&mut head_after, &[33]);
    let (head, prev) = boundary_streams(head_after, completed_ratified(33));
    let err = checked_s(&head, &prev, unbalanced_expected()).unwrap_err();
    assert!(err.contains("outside the ratified"), "{err}");
}

/// #1764 (review): the ratified instance's terminal epoch is derived from the
/// side's own terminal events, so it is re-based even once the instance is
/// evicted AND its key has wrapped out of `recently_terminated`.
#[test]
fn prev_release_gate_boundary_projection_terminal_epoch_after_ring_wrap() {
    let later = |mut s: serde_json::Value, evict: bool| {
        let state = s["state"]["state"].as_object_mut().expect("state");
        if evict {
            state["instances"]
                .as_object_mut()
                .expect("instances")
                .remove("10");
            state["recently_terminated"]
                .as_array_mut()
                .expect("recently_terminated")
                .retain(|k| k.as_u64() != Some(10));
        }
        let epoch = state["terminal_epoch"].as_u64().expect("terminal_epoch") + 1;
        state["terminal_epoch"] = serde_json::json!(epoch);
        state["instances"]["30"]["terminated_epoch"] = serde_json::json!(epoch);
        state["recently_terminated"]
            .as_array_mut()
            .expect("recently_terminated")
            .push(serde_json::json!(30u64));
        s
    };
    let (head, prev) = boundary_streams_with(
        later(with_ratified(33), false),
        later(completed_ratified(33), true),
        vec![terminated(30)],
        vec![completed(10), terminated(30)],
    );
    let mut expected = unbalanced_expected();
    expected["only_in_prev"] = serde_json::json!([{"variant": "ProcessInstanceCompleted"}]);
    checked_s(&head, &prev, expected)
        .unwrap_or_else(|e| panic!("a wrapped-out ratified completion reconciles: {e}"));
}

/// #1608 review: `settled_orphan_episodes` maps an instance key to an episode
/// key. A ratified remint shifts BOTH, so equivalent state on the two sides
/// must project equal — the value canonicalized as well as the entry key —
/// while a ratified instance's entry is still dropped. A tombstone's nested
/// `episode` is canonicalized the same way.
#[test]
fn settled_orphan_episodes_project_equal_under_remint() {
    use std::collections::{BTreeMap, BTreeSet};
    let ids = |pairs: &[(u64, &str)]| -> BTreeMap<u64, String> {
        pairs.iter().map(|(k, v)| (*k, (*v).to_string())).collect()
    };
    // Head: instance 10 settled at episode 15; ratified instance 30 at 31.
    let head_ids = ids(&[(10, "inst-A"), (15, "inc-A"), (30, "inst-R")]);
    // Prev: the same entities reminted to different raw keys.
    let prev_ids = ids(&[(20, "inst-A"), (25, "inc-A"), (40, "inst-R")]);
    let head = serde_json::json!({"10": 15, "30": 31});
    let prev = serde_json::json!({"20": 25, "40": 41});
    let excluded_head = BTreeSet::from([30]);
    let excluded_prev = BTreeSet::from([40]);
    let field = "settled_orphan_episodes";
    let h = compare::project_state_field(field, &head, &head_ids, &excluded_head).unwrap();
    let p = compare::project_state_field(field, &prev, &prev_ids, &excluded_prev).unwrap();
    assert_eq!(h, p, "a remint must not split equivalent settled episodes");
    assert_eq!(h, serde_json::json!({field: {"inst-A": "inc-A"}}));
    // A genuinely different episode still diverges.
    let other = serde_json::json!({"20": 26});
    let o = compare::project_state_field(field, &other, &prev_ids, &excluded_prev).unwrap();
    assert_ne!(h, o);

    // The tombstone's nested `episode` reference is canonicalized too.
    let field = "variables_lost_quarantined";
    let head = serde_json::json!({"10": {"incident_key": 15, "episode": 15}});
    let prev = serde_json::json!({"20": {"incident_key": 25, "episode": 25}});
    let h = compare::project_state_field(field, &head, &head_ids, &excluded_head).unwrap();
    let p = compare::project_state_field(field, &prev, &prev_ids, &excluded_prev).unwrap();
    assert_eq!(
        h, p,
        "a remint must not split equivalent tombstone episodes"
    );
}

/// The post-latch corpus (#2004, #2047 review): generated under the
/// `event-gateway-runtime-v1` envelope with keys harvested from the latched
/// engine, it latches the new runtime and runs a gateway-owned trigger, and a
/// replay under the same envelope is reproducible and restart-invisible —
/// so the same-release cells
/// determinism-check the latched path, not only the pre-latch one.
#[test]
fn post_latch_corpus_exercises_the_runtime_and_replays_deterministically() {
    let (entries, labels) = corpus::build_labeled_with(RANDOM, corpus::Runtime::PostLatch);
    let jsonl = corpus::to_jsonl(&entries, &labels);
    let opts = |stop_after, start_at| runner::RunOptions {
        state_every: 16,
        stop_after,
        start_at,
        event_gateway_runtime: true,
        ..Default::default()
    };
    let full = run(&jsonl, &opts(None, 0), &mut Engine::new());
    assert_eq!(full, run(&jsonl, &opts(None, 0), &mut Engine::new()));
    for event in ["EventGatewayRuntimeActivated", "EventGatewayTriggered"] {
        assert!(
            full.contains(event),
            "post-latch replay never emits {event}"
        );
    }
    // The pre-latch replay of the default corpus never latches.
    assert!(!full_run(&corpus_jsonl(), false).contains("EventGatewayRuntimeActivated"));

    let total = jsonl.lines().count() as u64;
    for split in [1, total / 3, total / 2 + 1] {
        let mut first_engine = Engine::new();
        let first = run(&jsonl, &opts(Some(split), 0), &mut first_engine);
        let snap = runner::save_snapshot(&first_engine).unwrap();
        let mut resumed = runner::load_snapshot(&snap).unwrap();
        let second = run(&jsonl, &opts(None, split), &mut resumed);
        assert_eq!(
            full,
            format!("{first}{second}"),
            "restart at {split} diverged"
        );
    }
}
