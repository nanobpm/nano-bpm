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
import { artifactsFor, exploreOrders, materialize } from "./enumerate.mjs";
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

const flowsOf = (graph, run) => {
  const byId = Object.fromEntries(graph.edges.map((e) => [e.id, e]));
  return Object.fromEntries(Object.entries(run.takenFlows).map(([id, n]) => [`${byId[id].from}->${byId[id].to}`, n]));
};

test("an event-based gateway takes no gateway->catch flow (#2004, Zeebe)", () => {
  // The reference, the engine and `ZeebeTokenFlow.CorrelateEvent` all activate
  // the triggered catch directly: no `G->M*` flow, the losing catch never
  // activated nor completed.
  const graph = materialize({ kinds: ["evt"], edges: [[0, 1, "direct"], [1, 2, "msg"], [1, 2, "msg"]] });
  const runs = exploreOrders(graph);
  assert.equal(runs.length, 2, "one run per catch");
  for (const run of runs) {
    const fromGw = Object.keys(flowsOf(graph, run)).filter((k) => graph.nodes[k.split("->")[0]] === "evt");
    assert.deepEqual(fromGw, [], "no gateway->catch flow taken");
    const catches = Object.keys(run.completedElements).filter((n) => graph.nodes[n] === "msg");
    assert.equal(catches.length, 1, "only the winning catch completes");
    const expected = referenceMilestones(graph, run);
    const wire = { completed: true, sequenceFlows: { ...expected.sequenceFlows }, jobsCreated: {} };
    assert.ok(checkSpecOracle(graph, run, wire).ok);
    // Red/Green: the legacy #1676 race (every gateway flow taken) is not a
    // spec behaviour.
    const legacy = { ...wire, sequenceFlows: { ...wire.sequenceFlows } };
    for (const e of graph.edges) if (graph.nodes[e.from] === "evt") legacy.sequenceFlows[`${e.from}->${e.to}`] = 1;
    const bad = checkSpecOracle(graph, run, legacy);
    assert.ok(!bad.ok, "the legacy race must fail the spec oracle");
    assert.ok(bad.mismatches.some((m) => m.field === "sequenceFlows"));
  }
  // A backend that continued down the LOSING catch fails too.
  const [r0, r1] = runs;
  const wrong = { completed: true, sequenceFlows: referenceMilestones(graph, r1).sequenceFlows, jobsCreated: {} };
  assert.ok(!checkSpecOracle(graph, r0, wrong).ok);
});

test("a cancelled waiting event-based gateway never completes and takes no flow (#2004)", () => {
  const graph = materialize({ kinds: ["evt"], edges: [[0, 1, "task"], [1, 2, "msg"], [1, 2, "msg"]] });
  const run = exploreOrders(graph, {}, { cancel: true }).find(
    (r) => r.steps.length === 2 && r.steps[0].op === "complete" && r.steps[1].op === "cancel",
  );
  assert.ok(run, "complete-then-cancel run explored");
  const gw = Object.keys(graph.nodes).find((n) => graph.nodes[n] === "evt");
  assert.equal(run.completedElements[gw], undefined, "the waiting gateway is terminated");
  assert.deepEqual(
    Object.keys(flowsOf(graph, run)).filter((k) => k.startsWith(`${gw}->`)),
    [],
  );
});

test("one message publish is ONE correlation per instance in the spec oracle, as Zeebe (#1988, #1793)", () => {
  // and,xor|0>1d,1>2d,1>2d,2>3n: two tokens park on T1, whose interrupting
  // message boundary B1 routes to E. Zeebe (MessageCorrelateBehavior) correlates
  // a publish once per process, so the run "publish B1, then cancel" has ONE
  // B1->E flow and never completes; Nano's matching trace must conform.
  const graph = materialize({
    kinds: ["and", "xor"],
    edges: [[0, 1, "direct"], [1, 2, "direct"], [1, 2, "direct"], [2, 3, "bmsg"]],
  });
  const run = exploreOrders(graph, {}, { cancel: true }).find(
    (r) => r.steps.length === 2 && r.steps[0].op === "correlateBoundary" && r.steps[1].op === "cancel",
  );
  assert.ok(run, "publish-then-cancel run explored");
  const expected = referenceMilestones(graph, run);
  assert.equal(expected.completed, false);
  assert.equal(expected.sequenceFlows["B1->E"], 1);
  const zeebeLike = {
    completed: false,
    sequenceFlows: { ...expected.sequenceFlows },
    jobsCreated: { ...expected.jobsCreated },
  };
  assert.equal(checkSpecOracle(graph, run, zeebeLike).ok, true);
  // The pre-#1988 every-subscription expectation no longer conforms.
  const everySub = { ...zeebeLike, completed: true, sequenceFlows: { ...zeebeLike.sequenceFlows, "B1->E": 2 } };
  assert.equal(checkSpecOracle(graph, run, everySub).ok, false);
});
