//! Trace-validation conformance tests (#1226, Deliverable B): the TLA+
//! TokenFlow spec anchored to the real engine.
//!
//! TLC emits the shortest completing behaviour of each trace-anchored model as a
//! committed JSON fixture (`formal/tla/traces/TokenFlow/*.json`, produced by
//! `formal/tla/gen-traces.sh`). Each fixture records the process graph plus the
//! multiset of observable milestones (tokens on flows, task wait states, join
//! firings, completion). This test rebuilds each model as a real engine process,
//! drives it to quiescence through `Engine::apply_command`, and asserts the
//! engine's observed milestone multiset **equals** the spec's — a divergence
//! fails the test (no tolerated mismatch, no retries).
//!
//! The replay driver lives in `harness` and is spec-agnostic; TokenFlow's
//! binding lives in `token_flow`. Sibling specs (#1227, #1240) reuse `harness`
//! with their own `TraceMapping`.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
#[path = "trace_validation/harness.rs"]
mod harness;
#[path = "trace_validation/token_flow.rs"]
mod token_flow;

use std::path::PathBuf;

use harness::{Fixture, Milestone, TraceMapping};
use token_flow::TokenFlowMapping;

fn traces_dir() -> PathBuf {
    // engine-core/tests/.. -> repo root -> formal/tla/traces/TokenFlow
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("engine-core has a parent (repo root)")
        .join("formal/tla/traces/TokenFlow")
}

fn load_fixtures() -> Vec<Fixture> {
    let dir = traces_dir();
    let mut fixtures = Vec::new();
    let mut entries: Vec<_> = std::fs::read_dir(&dir)
        .unwrap_or_else(|e| panic!("read {}: {e}", dir.display()))
        .map(|r| r.expect("dir entry").path())
        .filter(|p| p.extension().is_some_and(|x| x == "json"))
        .collect();
    entries.sort();
    for path in entries {
        let text = std::fs::read_to_string(&path).expect("read fixture");
        fixtures.push(
            Fixture::from_json(&text).unwrap_or_else(|e| panic!("parse {}: {e}", path.display())),
        );
    }
    assert!(
        !fixtures.is_empty(),
        "no committed TokenFlow trace fixtures found under {}",
        dir.display()
    );
    fixtures
}

/// Every committed TokenFlow behaviour replays against the engine with a
/// matching milestone multiset.
#[test]
fn tokenflow_traces_conform_to_engine() {
    let mapping = TokenFlowMapping;
    for fixture in load_fixtures() {
        harness::validate(&mapping, &fixture)
            .unwrap_or_else(|e| panic!("{}/{}: {e}", fixture.spec, fixture.model));
    }
}

/// The harness must actually *catch* a divergence — a milestone-multiset
/// mismatch has to fail, or the conformance test above would be vacuous. We wrap
/// TokenFlow's mapping in one that drops a milestone from the engine side and
/// assert `validate` reports the divergence (Red/Green: the detector is proven
/// to fire, permanently green).
#[test]
fn harness_detects_injected_divergence() {
    struct DropsAJoinFiring(TokenFlowMapping);
    impl TraceMapping for DropsAJoinFiring {
        fn process_id(&self, f: &Fixture) -> String {
            self.0.process_id(f)
        }
        fn build_process(&self, f: &Fixture) -> nanobpmn_engine_core::ProcessDefinition {
            self.0.build_process(f)
        }
        fn engine_milestones(&self, events: &[nanobpmn_engine_core::Event]) -> Vec<Milestone> {
            let mut ms = self.0.engine_milestones(events);
            // Drop the first join firing the engine reported: the spec still
            // expects it, so the multisets must diverge.
            if let Some(pos) = ms
                .iter()
                .position(|m| matches!(m, Milestone::JoinFired { .. }))
            {
                ms.remove(pos);
            }
            ms
        }
    }

    let fixtures = load_fixtures();
    let fixture = fixtures
        .iter()
        .find(|f| {
            f.milestones
                .iter()
                .any(|m| matches!(m, Milestone::JoinFired { .. }))
        })
        .expect("a trace-anchored model with a join firing");

    let broken = DropsAJoinFiring(TokenFlowMapping);
    let result = harness::validate(&broken, fixture);
    assert!(
        result.is_err(),
        "harness must fail when the engine milestone multiset diverges from the spec's"
    );
    assert!(
        result.unwrap_err().contains("divergence"),
        "the divergence report should name the mismatch"
    );
}
