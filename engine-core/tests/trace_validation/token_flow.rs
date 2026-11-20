//! The TokenFlow spec family's [`TraceMapping`]: how nano's `TokenFlow.tla`
//! vocabulary binds to the real engine (#1226). This is the reference
//! implementation sibling specs copy the *shape* of, while reusing the
//! `harness` driver itself.

use nanobpmn_engine_core::{Event, ProcessDefinition};

use super::harness::{Fixture, Milestone, TraceMapping};

/// TokenFlow → engine binding.
///
/// Node kinds map to engine elements as:
///   * `start` → start event, `end` → end event
///   * `task`  → **service task** (a wait state that creates a job; the harness
///     drives it to completion), job type = the node id
///   * `and`   → parallel gateway, `or` → inclusive gateway, `xor` → exclusive
///     gateway
///
/// Only the parallel, routing-deterministic corpus is trace-anchored, so `or`/
/// `xor` never appear in a fixture graph; they are mapped for completeness (and
/// for siblings) but exercised only by the model checker, not the replay.
///
/// The process construction and engine-side projection are the canonical
/// shared ones (`harness::build_spec_process` / `harness::spec_engine_milestones`),
/// so this binding stays in lockstep with every sibling spec family (#1259).
pub struct TokenFlowMapping;

impl TraceMapping for TokenFlowMapping {
    fn process_id(&self, fixture: &Fixture) -> String {
        fixture.model.clone()
    }

    fn build_process(&self, fixture: &Fixture) -> ProcessDefinition {
        super::harness::build_spec_process(fixture)
    }

    fn engine_milestones(&self, events: &[Event]) -> Vec<Milestone> {
        super::harness::spec_engine_milestones(events)
    }
}
