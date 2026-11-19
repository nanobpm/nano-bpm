//! Trace-validation conformance for the **ZeebeTokenFlow** spec (#1259, epic
//! #1240): the Zeebe reference token flow anchored to both engines.
//!
//! This test reuses the spec-agnostic #1226 replay harness
//! (`engine-core/tests/trace_validation/harness.rs`) and the shared
//! `Milestone` vocabulary, exactly as `token_flow.rs` does. The difference is
//! the *oracle*: `ZeebeTokenFlow.tla` is the reference semantics derived from
//! `zeebe/engine`, so this test anchors it in two directions:
//!
//! 1. **Engine replay**: TLC-generated behaviours of the `ZMC*` corpus are
//!    replayed against `Engine::apply_command`, asserting the engine's
//!    observable milestone multiset equals the spec's. This ties the spec to
//!    nano.
//! 2. **Spec-level normalisation**: the captured Zeebe golden traces (the
//!    `formal/traces/goldens/*.trace.json` record streams, normalised by
//!    `formal/traces/normaliser.json`) are projected onto the same milestone
//!    vocabulary by `zeebe_token_flow::normalised_records_to_milestones`. This
//!    ties the spec to Zeebe.
//!
//! Both directions share the `TraceMapping` trait shape and the `Milestone`
//! vocabulary, so the harness's `validate` driver is reused unchanged. A
//! divergence in either direction fails the test — no tolerated mismatch, no
//! retries.

#![allow(
    clippy::unwrap_used,
    clippy::expect_used,
    reason = "non-production target (integration tests / examples); the production deny-unwrap gate (#1357) applies to the library crate only"
)]
#[path = "trace_validation/harness.rs"]
mod harness;
#[path = "trace_validation/zeebe_token_flow.rs"]
mod zeebe_token_flow;

use std::path::PathBuf;

use harness::{Fixture, Milestone, TraceMapping};
use zeebe_token_flow::ZeebeTokenFlowMapping;

fn traces_dir() -> PathBuf {
    // engine-core/tests/.. -> repo root -> formal/tla/traces/ZeebeTokenFlow
    PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .expect("engine-core has a parent (repo root)")
        .join("formal/tla/traces/ZeebeTokenFlow")
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
        "no committed ZeebeTokenFlow trace fixtures found under {}",
        dir.display()
    );
    fixtures
}

/// Every committed ZeebeTokenFlow behaviour replays against the engine with a
/// matching milestone multiset.
#[test]
fn zeebe_tokenflow_traces_conform_to_engine() {
    let mapping = ZeebeTokenFlowMapping;
    for fixture in load_fixtures() {
        harness::validate(&mapping, &fixture)
            .unwrap_or_else(|e| panic!("{}/{}: {e}", fixture.spec, fixture.model));
    }
}

/// The harness must actually *catch* a divergence — a milestone-multiset
/// mismatch has to fail, or the conformance test above would be vacuous. We wrap
/// ZeebeTokenFlow's mapping in one that drops a milestone from the engine side
/// and assert `validate` reports the divergence (Red/Green: the detector is
/// proven to fire, permanently green).
#[test]
fn harness_detects_injected_divergence() {
    struct DropsAJoinFiring(ZeebeTokenFlowMapping);
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

    let broken = DropsAJoinFiring(ZeebeTokenFlowMapping);
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
