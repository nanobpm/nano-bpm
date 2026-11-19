//! Canonical snapshot encoding (ADR 0066 slice 3, #1662).
//!
//! Replicas that hold **equal** engine state must serialize it to
//! **byte-identical** snapshots — the divergence digest (ADR 0066 slice 5)
//! hashes those bytes. With std `HashMap`/`HashSet` (per-process `RandomState`)
//! the serialized element order depended on the hasher seed, the table's
//! capacity and its mutation history, so equal states produced different bytes.
//! Engine state now lives in ordered collections (`BTreeMap`/`BTreeSet`), whose
//! iteration — and therefore serialization — order is a function of the
//! contents alone.
//!
//! Each case below reaches one logical state through two different histories
//! (live apply vs. full replay vs. snapshot-restore + tail, and different
//! variable-mutation orders) and asserts the encodings are byte-identical.

#![cfg(feature = "serde")]
#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]

use std::collections::BTreeMap;

use nanobpmn_engine_core::{Command, Engine, Event, ProcessBuilder, Value};

const T0: u64 = 1_700_000_000_000;

fn state_bytes(engine: &Engine) -> Vec<u8> {
    serde_json::to_vec(engine.state()).expect("state serializes")
}

fn snapshot_bytes(engine: &Engine) -> Vec<u8> {
    serde_json::to_vec(&engine.snapshot()).expect("snapshot serializes")
}

fn vars(pairs: &[(&str, i64)]) -> BTreeMap<String, Value> {
    pairs
        .iter()
        .map(|(k, v)| ((*k).to_string(), Value::Int(*v)))
        .collect()
}

/// Deploys a parallel process and starts `n` instances, each parked on two
/// jobs, so the state carries many entries in every major map (instances,
/// jobs, activatable-job index, per-instance indexes, variables).
fn populated(n: u64) -> (Engine, Vec<Event>, Vec<u64>) {
    let mut engine = Engine::new();
    let mut events = Vec::new();
    let def = ProcessBuilder::new("fan")
        .start_event("s")
        .parallel_gateway("split")
        .service_task("a", "type-a")
        .service_task("b", "type-b")
        .parallel_gateway("join")
        .end_event("e")
        .connect("s", "split")
        .connect("split", "a")
        .connect("split", "b")
        .connect("a", "join")
        .connect("b", "join")
        .connect("join", "e")
        .build()
        .expect("build");
    events.extend(
        engine
            .apply_command_at(Command::DeployProcess(def), T0)
            .expect("deploy"),
    );
    let mut instances = Vec::new();
    for i in 0..n {
        let created = engine
            .apply_command_at(
                Command::create_instance_with(
                    "fan",
                    vars(&[("i", i as i64), ("z", 1), ("m", 2), ("a", 3)]),
                ),
                T0 + 1 + i,
            )
            .expect("create");
        for e in &created {
            if let Event::ProcessInstanceCreated { instance_key, .. } = e {
                instances.push(*instance_key);
            }
        }
        events.extend(created);
    }
    (engine, events, instances)
}

/// Live apply, full replay, and snapshot-restore + tail replay — at every split
/// point — all encode the same state to the same bytes.
#[test]
fn live_replay_and_snapshot_restore_encode_identically() {
    let (live, events, _) = populated(24);
    let expected = state_bytes(&live);

    let replayed = Engine::replay(events.iter().cloned());
    assert_eq!(
        state_bytes(&replayed),
        expected,
        "full replay must encode byte-identically to live apply"
    );

    for k in 0..=events.len() {
        let head = Engine::replay(events[..k].iter().cloned());
        let raw = snapshot_bytes(&head);
        let snap = serde_json::from_slice(&raw).expect("snapshot deserializes");
        let mut restored = Engine::from_snapshot(snap);
        // Restore must re-encode to the very bytes it was decoded from.
        assert_eq!(
            snapshot_bytes(&restored),
            raw,
            "split k={k}: snapshot must round-trip byte-identically"
        );
        restored.apply_replayed_events(events[k..].iter().cloned());
        assert_eq!(
            state_bytes(&restored),
            expected,
            "split k={k}: snapshot-restore + tail must encode byte-identically to live apply"
        );
    }
}

/// Two engines that reach equal state through different mutation orders
/// (every instance's variables written in opposite orders) encode identically.
#[test]
fn different_mutation_histories_encode_identically() {
    let (mut left, _, instances) = populated(16);
    let (mut right, _, instances_r) = populated(16);
    assert_eq!(instances, instances_r);

    let now = T0 + 10_000;
    let names = ["q", "b", "x", "c", "y", "d", "w", "e"];
    for &key in &instances {
        for (n, name) in names.iter().enumerate() {
            left.apply_command_at(Command::set_variables(key, vars(&[(name, n as i64)])), now)
                .expect("set var");
        }
        for (n, name) in names.iter().enumerate().rev() {
            right
                .apply_command_at(Command::set_variables(key, vars(&[(name, n as i64)])), now)
                .expect("set var");
        }
    }
    assert_eq!(left.state(), right.state(), "histories reach equal state");
    assert_eq!(
        state_bytes(&left),
        state_bytes(&right),
        "equal state reached through different mutation histories must encode byte-identically"
    );
}
