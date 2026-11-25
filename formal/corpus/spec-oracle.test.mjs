// Tests for the ZeebeTokenFlow spec oracle (#1261, slice 6 of #1240).
//
// The oracle asserts a backend's observation is a BEHAVIOUR of
// `ZeebeTokenFlow.tla` by comparing its milestone multiset against the run the
// scenario was generated from (a `Reference` run IS a spec behaviour). These
// tests pin the milestone projection, the endpoint-collapsing grammar, and —
// Red/Green — that the oracle actually FIRES on a trace that is not a spec
// behaviour.

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import { artifactsFor } from "./enumerate.mjs";
import {
  referenceMilestones,
  observationMilestones,
  checkSpecOracle,
} from "./spec-oracle.mjs";

const graphsDir = fileURLToPath(new URL("./graphs", import.meta.url));
const loadGraph = (id) =>
  JSON.parse(readFileSync(join(graphsDir, `${id}.json`), "utf8"));

test("a parallel diamond's run projects to the ZeebeTokenFlow milestones", () => {
  const graph = loadGraph("ParallelDiamond");
  const { scenarios } = artifactsFor(graph);
  assert.ok(scenarios.length > 0);
  for (const sc of scenarios) {
    const ms = referenceMilestones(graph, sc.run);
    // The split takes S->P1, P1->A, P1->B; the join fires once (P2), consuming
    // A->P2 and B->P2, then takes P2->E; the instance completes.
    assert.equal(ms.completed, true);
    assert.deepEqual(ms.jobsCreated, { A: 1, B: 1 });
    assert.deepEqual(ms.joinsFired, { P2: 1 });
    assert.deepEqual(ms.sequenceFlows, {
      "S->P1": 1,
      "P1->A": 1,
      "P1->B": 1,
      "A->P2": 1,
      "B->P2": 1,
      "P2->E": 1,
    });
  }
});

test("the milestone projection collapses a taken flow to its (from,to) endpoints", () => {
  // Two distinct flows sharing endpoints are ONE `from->to` milestone — nano's
  // SequenceFlowTaken carries no per-flow identity (the documented #1227 gap).
  const graph = loadGraph("ParallelDuplicateFlows");
  const { scenarios } = artifactsFor(graph);
  for (const sc of scenarios) {
    const ms = referenceMilestones(graph, sc.run);
    // Every milestone key is an endpoint pair, never a raw flow id.
    for (const key of Object.keys(ms.sequenceFlows)) {
      assert.match(key, /^[^-]+->[^-]+$/, `endpoint-keyed: ${key}`);
    }
  }
});

test("a matching observation IS a behaviour of the spec (oracle passes)", () => {
  const graph = loadGraph("ParallelDiamond");
  const { scenarios } = artifactsFor(graph);
  for (const sc of scenarios) {
    // The scenario's `expect` block IS the reference observation (the same
    // projection), so feeding it back must pass the oracle.
    const obs = {
      completed: sc.expect.completed,
      sequenceFlows: sc.expect.sequenceFlows,
      jobsCreated: sc.expect.jobsCreated,
    };
    const res = checkSpecOracle(graph, sc.run, obs);
    assert.ok(
      res.ok,
      `${sc.name} should be a spec behaviour: ${JSON.stringify(res.mismatches)}`,
    );
  }
});

test("Red/Green: the oracle FIRES on a trace that is not a spec behaviour", () => {
  const graph = loadGraph("ParallelDiamond");
  const { scenarios } = artifactsFor(graph);
  const sc = scenarios[0];

  // A join that fired WITHOUT taking both incoming flows is not a ZeebeTokenFlow
  // behaviour (it violates `ParallelJoinWaitsForEveryFlow`): drop one incoming
  // flow from the observation and the oracle must report it.
  const missingFlow = {
    completed: sc.expect.completed,
    sequenceFlows: { ...sc.expect.sequenceFlows, "B->P2": 0 },
    jobsCreated: sc.expect.jobsCreated,
  };
  const r1 = checkSpecOracle(graph, sc.run, missingFlow);
  assert.ok(!r1.ok, "a missing taken flow must fail the spec oracle");
  assert.ok(
    r1.mismatches.some((m) => m.field === "sequenceFlows"),
    "the mismatch must name sequenceFlows",
  );

  // A task that never reached its wait state is not a spec behaviour.
  const missingTask = {
    completed: sc.expect.completed,
    sequenceFlows: sc.expect.sequenceFlows,
    jobsCreated: { A: 1 }, // B never activated
  };
  const r2 = checkSpecOracle(graph, sc.run, missingTask);
  assert.ok(!r2.ok, "a missing task wait state must fail the spec oracle");

  // An instance that did NOT complete when the spec says it must is not a spec
  // behaviour.
  const notCompleted = {
    completed: false,
    sequenceFlows: sc.expect.sequenceFlows,
    jobsCreated: sc.expect.jobsCreated,
  };
  const r3 = checkSpecOracle(graph, sc.run, notCompleted);
  assert.ok(!r3.ok, "a non-completing trace must fail the spec oracle");
});

test("observationMilestones projects only the shared token-flow fields", () => {
  const ms = observationMilestones({
    completed: true,
    sequenceFlows: { "A->B": 2 },
    jobsCreated: { A: 1 },
    // Fields outside the token-flow vocabulary are dropped:
    variables: { x: 1 },
    completedElements: { A: 1 },
    incidents: { SOME_ERROR: 1 },
  });
  assert.deepEqual(ms, {
    completed: true,
    sequenceFlows: { "A->B": 2 },
    jobsCreated: { A: 1 },
  });
});

test("an event-based gateway's expectation is the SPEC projection, not the engine record", () => {
  // #1248 review: `Reference.apply()` records every outgoing flow of a
  // correlated event-based gateway (the engine arms every catch and withdraws
  // the losers, taking each losing catch's gateway flow), while
  // `ZeebeTokenFlow.CorrelateEvent` takes only the selected flow. The oracle
  // must expect the spec's projection, or a backend that takes the losing
  // flows passes as "a behaviour of the spec" (the #1676 divergence shape).
  const graph = {
    id: "EvtChoice",
    nodes: { S: "start", G: "evt", M1: "msg", M2: "msg", E: "end" },
    edges: [
      { id: "f0", from: "S", to: "G" },
      { id: "f1", from: "G", to: "M1" },
      { id: "f2", from: "G", to: "M2" },
      { id: "f3", from: "M1", to: "E" },
      { id: "f4", from: "M2", to: "E" },
    ],
  };
  // The run correlates M1 (selecting f1): the engine-shaped record also takes
  // the losing f2 and completes M2 (withdrawn). Zeebe takes NEITHER gateway
  // flow (it activates the triggered catch directly — #1962's exporter-trace
  // validation), so the spec projection has no `G->M*` at all.
  const run = {
    steps: [{ op: "correlate", node: "G", flow: "f1", catch: "M1" }],
    route: {},
    completed: true,
    jobsCreated: {},
    completedElements: { S: 1, G: 1, M1: 1, M2: 1, E: 1 },
    takenFlows: { f0: 1, f1: 1, f2: 1, f3: 1 },
    joinsFired: {},
  };
  const ms = referenceMilestones(graph, run);
  assert.deepEqual(ms.sequenceFlows, {
    "S->G": 1,
    "M1->E": 1,
    // No "G->M1"/"G->M2": the spec never takes a gateway->catch flow.
  });

  // The engine's WIRE record takes both gateway flows (arm all, withdraw the
  // loser) — exactly what `expect.sequenceFlows` pins. The spec oracle must
  // ACCEPT it (both oracles see the same observation; a contradiction would
  // fail every event-gateway scenario in CI).
  const wire = {
    completed: true,
    sequenceFlows: { "S->G": 1, "G->M1": 1, "G->M2": 1, "M1->E": 1 },
    jobsCreated: {},
  };
  const ok = checkSpecOracle(graph, run, wire);
  assert.ok(ok.ok, JSON.stringify(ok.mismatches));

  // Red/Green: a backend that continued down the LOSING catch is not a
  // behaviour of the spec's selection and must fail.
  const wrongCatch = {
    completed: true,
    sequenceFlows: { "S->G": 1, "G->M1": 1, "G->M2": 1, "M2->E": 1 },
    jobsCreated: {},
  };
  const bad = checkSpecOracle(graph, run, wrongCatch);
  assert.ok(!bad.ok, "continuing down the losing catch must fail the spec oracle");
  assert.ok(bad.mismatches.some((m) => m.field === "sequenceFlows"));
});

test("event-gateway selections show only through each catch's continuation", () => {
  // Gateway activated twice, selecting each alternative once: the spec takes
  // no gateway->catch flow (not the raw withdrawal-inclusive 2/2); each
  // selection is visible as its catch's continuation, once each.
  const graph = {
    id: "EvtLoop",
    nodes: { S: "start", G: "evt", M1: "msg", M2: "msg", E: "end" },
    edges: [
      { id: "f0", from: "S", to: "G" },
      { id: "f1", from: "G", to: "M1" },
      { id: "f2", from: "G", to: "M2" },
      { id: "f3", from: "M1", to: "E" },
      { id: "f4", from: "M2", to: "E" },
    ],
  };
  const run = {
    steps: [
      { op: "correlate", node: "G", flow: "f1", catch: "M1" },
      { op: "correlate", node: "G", flow: "f2", catch: "M2" },
    ],
    completed: true,
    jobsCreated: {},
    takenFlows: { f0: 2, f1: 2, f2: 2, f3: 1, f4: 1 },
    joinsFired: {},
  };
  const ms = referenceMilestones(graph, run);
  assert.equal(ms.sequenceFlows["G->M1"], undefined);
  assert.equal(ms.sequenceFlows["G->M2"], undefined);
  assert.equal(ms.sequenceFlows["M1->E"], 1);
  assert.equal(ms.sequenceFlows["M2->E"], 1);
  const wire = {
    completed: true,
    sequenceFlows: { "S->G": 2, "G->M1": 2, "G->M2": 2, "M1->E": 1, "M2->E": 1 },
    jobsCreated: {},
  };
  const r = checkSpecOracle(graph, run, wire);
  assert.ok(r.ok, JSON.stringify(r.mismatches));
});
