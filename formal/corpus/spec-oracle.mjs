// The ZeebeTokenFlow spec oracle (#1261, slice 6 of #1240): per scenario, the
// trace a backend emits must be a BEHAVIOUR of `ZeebeTokenFlow.tla` — the Zeebe
// reference semantics derived from `zeebe/engine` (#1240 slice 1). This is the
// *spec* oracle, run alongside the *differential* oracle (`observation.mjs`'s
// `diffObservations`, which asserts the two backends' outcomes are identical).
//
// The spec vocabulary may only use what BOTH engines expose (#1240). The
// observable milestones are `ZeebeTokenFlow.tla`'s observable projection — the
// same vocabulary the Rust trace-validation harness
// (`engine-core/tests/trace_validation/zeebe_token_flow.rs`) projects the
// normalised record stream onto:
//
//   | Spec variable | Exporter record                    | Milestone           |
//   |---------------|------------------------------------|---------------------|
//   | `taken`       | `SEQUENCE_FLOW_TAKEN`              | `Flow { from, to }` |
//   | `active`      | `ELEMENT_ACTIVATED` of a task      | `Task { node }`     |
//   | `fired`       | `ELEMENT_ACTIVATED` of a join      | `JoinFired { node }`|
//   | `done`        | `ELEMENT_COMPLETED` of the process | `Completed`         |
//
// THE REFERENCE SIDE needs no TLC run: `enumerate.mjs`'s `Reference` is already
// an executable ZeebeTokenFlow — its `joinFires` is Zeebe's arrival-time
// inclusive/parallel join guard (`canActivateParallelGateway` /
// `canActivateInclusiveGateway`, #1241), its `take` records every taken flow,
// and its settle drains pass-throughs exactly as the spec's transition relation
// does. The run a scenario was generated from (`scenariosFor` → `exploreOrders`,
// attached as `scenario.run`) is therefore BY CONSTRUCTION a behaviour of
// `ZeebeTokenFlow`. Projecting that run to milestones gives the spec's
// expectation; projecting a backend's observation to the same vocabulary and
// asserting the multisets are EQUAL is the check that the backend's trace is a
// behaviour of the spec.
//
// The two projections agree on the milestone grammar: both collapse a taken flow
// to its `(from, to)` endpoints — nano's `SequenceFlowTaken` event carries no
// per-flow identity (the documented #1227 gap), so two distinct flows sharing
// endpoints are one `from->to` milestone on both sides. The Camunda backend (a
// bare gateway, #1260) exposes only completion + variables, so it carries no
// milestone projection here; the spec oracle runs on the nano side (the
// always-on reference), with the differential oracle covering Camunda.

import { diffObservations, emptyObservation } from "../parity/runtime/observation.mjs";

/**
 * The milestone multiset a `Reference` run exhibits, in the observation
 * vocabulary (so it is compared with `diffObservations`' canonical multiset
 * equality):
 *
 *   - `sequenceFlows`:  `from->to` -> count   (the spec's `taken`)
 *   - `jobsCreated`:    task node -> count    (the spec's `active` wait states)
 *   - `joinsFired`:     join node -> count    (the spec's `fired` ghost)
 *   - `completed`:      the process instance's `done`
 *
 * `run` is one `exploreOrders(graph)` result: `{ route, steps, completed,
 * completedElements, takenFlows, jobsCreated, joinsFired }`. The taken-flow ids
 * are collapsed to their `(from, to)` endpoints through the graph's edge map —
 * the same endpoint-keyed form the nano observation uses.
 *
 * EVENT-BASED GATEWAYS take no `gateway->catch` flow in the reference, the
 * engine (#2004) or `ZeebeTokenFlow.CorrelateEvent`: the trigger activates the
 * winning catch directly (pinned against real Zeebe exporter records by the
 * #1962 trace validation). Which catch was selected shows in the catch's
 * continuation flow, so a backend that continues down a losing catch — or
 * that still takes the gateway's flows (the legacy #1676 race) — diverges.
 */
export function referenceMilestones(graph, run) {
  const byId = Object.fromEntries(graph.edges.map((e) => [e.id, e]));
  const sequenceFlows = {};
  for (const [fid, count] of Object.entries(run.takenFlows ?? {})) {
    const e = byId[fid];
    if (!e) continue;
    const key = `${e.from}->${e.to}`;
    sequenceFlows[key] = (sequenceFlows[key] ?? 0) + count;
  }
  return {
    completed: Boolean(run.completed),
    sequenceFlows,
    jobsCreated: { ...(run.jobsCreated ?? {}) },
    joinsFired: { ...(run.joinsFired ?? {}) },
  };
}

/**
 * Project a backend's observation (the differential oracle's vocabulary) onto
 * the milestone multiset. Nano exposes every field; the observation's
 * `sequenceFlows` is already `from->to`-keyed (matching the reference) and its
 * `jobsCreated` is task-node-keyed. `completed` is the process `done`.
 *
 * The observation carries no join-fired field (the two-backend observation
 * vocabulary has no `ParallelJoinFired`), so the `JoinFired` milestone is
 * checked on the reference side only via the run replay — see
 * `checkSpecOracle`'s note. What the observation CAN pin against the spec is
 * the taken-flow / task-wait / completion projection.
 */
export function observationMilestones(obs) {
  return {
    completed: Boolean(obs.completed),
    sequenceFlows: { ...(obs.sequenceFlows ?? {}) },
    jobsCreated: { ...(obs.jobsCreated ?? {}) },
  };
}

/**
 * The spec oracle for one scenario: assert the backend's observation is a
 * behaviour of `ZeebeTokenFlow` by comparing its milestone multiset against the
 * run the scenario was generated from. Returns `{ ok, mismatches,
 * comparedFields }` in the same shape as `diffObservations`, so the caller
 * reports it uniformly.
 *
 * Only the fields present in BOTH projections are compared (`completed`,
 * `sequenceFlows`, `jobsCreated`): the reference's `joinsFired` has no
 * observation counterpart (it is anchored to the spec separately, through the
 * Rust trace-validation harness and the TLC `JoinFiresAtMostOnce` invariant),
 * and the observation's `variables` / `completedElements` / `incidents` are
 * pinned by the `expect` block and the differential oracle, not the token-flow
 * spec. A non-empty `mismatches` means the backend's trace is NOT a behaviour
 * of `ZeebeTokenFlow` — a parity defect, no tolerance, no retry.
 *
 * SCOPE: this is a necessary-condition (count-projection) check, not a full
 * ordered refinement check. The two-backend observation vocabulary is a
 * multiset and carries no ordering, so a trace that reorders milestones while
 * preserving totals is NOT caught here. ORDERED validation of the record stream
 * against `ZeebeTokenFlow`'s transition relation is the job of the Rust
 * trace-validation harness (`engine-core/tests/trace_validation/zeebe_token_flow.rs`),
 * which replays each step against the spec's guards.
 */
export function checkSpecOracle(graph, run, observation) {
  const expected = referenceMilestones(graph, run);
  const actual = observationMilestones(observation);
  // Reuse the differential oracle's canonical multiset comparison over the
  // shared milestone fields, treating the two projections as two observations.
  const expObs = { ...emptyObservation(), ...expected };
  const actObs = { ...emptyObservation(), ...actual };
  const provide = new Set(["completed", "sequenceFlows", "jobsCreated"]);
  return diffObservations(actObs, expObs, provide, provide);
}
