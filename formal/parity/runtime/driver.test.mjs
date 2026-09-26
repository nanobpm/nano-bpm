// Tests for the two-backend parity runner (#1260).
//
// These exercise the nano backend (always available, no runtime) plus the
// backend-agnostic observation oracle. The live Camunda 8 differential is
// exercised by `run.mjs --backend both` in the CI job when a runtime is
// provisioned; here we test the oracle logic directly with synthetic
// observations so the differential's correctness is guarded without Docker.

import { test } from "node:test";
import assert from "node:assert/strict";
import { fileURLToPath } from "node:url";
import { createServer } from "node:http";
import { once } from "node:events";
import {
  NanoBackend,
  incidentsFromSnapshot,
  rootVariablesFromSearch,
} from "./nano-backend.mjs";
import { CamundaBackend } from "./camunda-backend.mjs";
import { runScenario } from "./driver.mjs";
import { discoverScenarios } from "./run.mjs";
import {
  checkExpectations,
  diffObservations,
  emptyObservation,
  variablesFromSearchItems,
} from "./observation.mjs";

const corpusDir = fileURLToPath(new URL("./corpus", import.meta.url));

test("every seed scenario satisfies its expect block on nano", async () => {
  const scenarios = discoverScenarios(corpusDir);
  assert.ok(scenarios.length >= 3, "expected at least three seed scenarios");
  const nano = await new NanoBackend().init();
  try {
    for (const scenario of scenarios) {
      const obs = await runScenario(nano, scenario);
      const check = checkExpectations(obs, scenario.expect);
      assert.ok(
        check.ok,
        `${scenario.name} expectation mismatch: ${JSON.stringify(check.mismatches)}`,
      );
    }
  } finally {
    await nano.close();
  }
});

test("nano runs are deterministic — a re-run reproduces byte-identical observations", async () => {
  const scenarios = discoverScenarios(corpusDir);
  const a = await new NanoBackend().init();
  const b = await new NanoBackend().init();
  try {
    for (const scenario of scenarios) {
      const obsA = await runScenario(a, scenario);
      const obsB = await runScenario(b, scenario);
      const diff = diffObservations(obsA, obsB, a.provides, b.provides);
      assert.ok(diff.ok, `${scenario.name} nondeterministic: ${JSON.stringify(diff.mismatches)}`);
    }
  } finally {
    await a.close();
    await b.close();
  }
});

test("diffObservations only compares fields BOTH backends provide", () => {
  const nano = emptyObservation();
  nano.completed = true;
  nano.variables = { x: 1 };
  nano.completedElements = { A: 1 };
  const camunda = emptyObservation();
  camunda.completed = true;
  camunda.variables = { x: 1 };
  // camunda does not populate completedElements and must not claim to.
  const provNano = new Set(["completed", "variables", "completedElements"]);
  const provCam = new Set(["completed", "variables"]);
  const diff = diffObservations(nano, camunda, provNano, provCam);
  assert.ok(diff.ok, JSON.stringify(diff.mismatches));
  assert.deepEqual(diff.comparedFields.sort(), ["completed", "variables"]);
});

test("diffObservations flags a real variable divergence", () => {
  const a = emptyObservation();
  a.completed = true;
  a.variables = { branch: "left" };
  const b = emptyObservation();
  b.completed = true;
  b.variables = { branch: "right" };
  const prov = new Set(["completed", "variables"]);
  const diff = diffObservations(a, b, prov, prov);
  assert.ok(!diff.ok);
  assert.equal(diff.mismatches.length, 1);
  assert.equal(diff.mismatches[0].field, "variables");
});

test("variable equality is insensitive to key order", () => {
  const a = emptyObservation();
  a.variables = { a: 1, b: { c: 2, d: 3 } };
  const b = emptyObservation();
  b.variables = { b: { d: 3, c: 2 }, a: 1 };
  const prov = new Set(["variables"]);
  assert.ok(diffObservations(a, b, prov, prov).ok);
});

test("checkExpectations rejects an unknown field", () => {
  const res = checkExpectations(emptyObservation(), { bogus: true });
  assert.ok(!res.ok);
  assert.match(res.mismatches[0].error, /unknown expect field/);
});

test("variablesFromSearchItems parses the C8 v2 variable shape", () => {
  const items = [
    { name: "a", value: "true" },
    { name: "nested", value: '{"k":[1,2]}' },
    { name: "s", value: '"hi"' },
  ];
  assert.deepEqual(variablesFromSearchItems(items), {
    a: true,
    nested: { k: [1, 2] },
    s: "hi",
  });
});

// --- Regression coverage for the #1260 review findings -----------------------

const INCIDENT_BPMN = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Definitions_incident" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="parity-incident" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="GW" />
    <bpmn:exclusiveGateway id="GW"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:exclusiveGateway>
    <bpmn:sequenceFlow id="f2" sourceRef="GW" targetRef="End">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">=missingVar = "x"</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>`;

test("nano observe records the incident kind (not UNKNOWN) keyed by instanceKey", async () => {
  // Guards the read-model snapshot field names: incidents are camelCased
  // `instanceKey`/`kind`, so reading `processInstanceKey`/`type` silently logged
  // every incident under UNKNOWN and matched no instance (#1260 review).
  const nano = await new NanoBackend().init();
  try {
    await nano.deploy(INCIDENT_BPMN);
    const handle = await nano.start("parity-incident", {});
    const obs = await nano.observe(handle);
    assert.deepEqual(obs.incidents, { noMatchingSequenceFlow: 1 });
    assert.ok(!("UNKNOWN" in obs.incidents), "must not fall back to UNKNOWN");
  } finally {
    await nano.close();
  }
});

test("nano observe returns a completed instance's full root-scope variables", async () => {
  // A completed instance's snapshot `variables` is emptied, so variables must be
  // read from the `searchVariables` surface, root-scope filtered to match
  // Camunda's `awaitCompletion` response (#1260 review).
  const scenario = {
    processId: "parity-linear",
    variables: { seed: "s", count: 3 },
    steps: [{ op: "activateAndComplete", jobType: "work", variables: { done: true } }],
    xml: `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Definitions_linear" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="parity-linear" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Task" />
    <bpmn:serviceTask id="Task"><bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Task" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>`,
  };
  const nano = await new NanoBackend().init();
  try {
    const obs = await runScenario(nano, scenario);
    assert.ok(obs.completed);
    assert.deepEqual(obs.variables, { seed: "s", count: 3, done: true });
  } finally {
    await nano.close();
  }
});

test("nano observe fails loudly on a truncated variable rather than diverging silently", async () => {
  // A value over the read-model preview length is truncated by `searchVariables`;
  // emitting the preview would silently diverge from Camunda's full value, so the
  // observation must throw instead (#1260 review).
  const big = "z".repeat(9000); // > VARIABLE_VALUE_PREVIEW_LEN (8192)
  const scenario = {
    processId: "parity-linear-big",
    variables: { big },
    steps: [{ op: "activateAndComplete", jobType: "work", variables: {} }],
    xml: `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Definitions_linear_big" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="parity-linear-big" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Task" />
    <bpmn:serviceTask id="Task"><bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Task" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
</bpmn:definitions>`,
  };
  const nano = await new NanoBackend().init();
  try {
    await assert.rejects(() => runScenario(nano, scenario), /truncated/);
  } finally {
    await nano.close();
  }
});

test("incidentsFromSnapshot reads camelCase instanceKey/kind", () => {
  const snapshot = {
    incidents: [
      { key: "6", instanceKey: "3", elementId: "GW", kind: "noMatchingSequenceFlow", reason: "…" },
      { key: "7", instanceKey: "9", elementId: "T", kind: "jobNoRetries", reason: "…" },
    ],
  };
  assert.deepEqual(incidentsFromSnapshot(snapshot, "3"), { noMatchingSequenceFlow: 1 });
  assert.deepEqual(incidentsFromSnapshot(snapshot, 3), { noMatchingSequenceFlow: 1 });
  assert.deepEqual(incidentsFromSnapshot(snapshot, "0"), {});
  assert.deepEqual(incidentsFromSnapshot({}, "3"), {});
});

test("rootVariablesFromSearch keeps only the root scope and drops nested scopes", () => {
  const items = [
    { name: "root", value: '"r"', processInstanceKey: "3", scopeKey: "3", isTruncated: false },
    { name: "nested", value: '"n"', processInstanceKey: "3", scopeKey: "42", isTruncated: false },
    { name: "other", value: '"o"', processInstanceKey: "9", scopeKey: "9", isTruncated: false },
  ];
  assert.deepEqual(rootVariablesFromSearch(items, "3"), { root: "r" });
  assert.deepEqual(rootVariablesFromSearch(items, 3), { root: "r" });
  assert.deepEqual(rootVariablesFromSearch([], "3"), {});
});

test("rootVariablesFromSearch throws on a truncated root-scope variable", () => {
  const items = [
    { name: "big", value: "zzz", processInstanceKey: "3", scopeKey: "3", isTruncated: true },
  ];
  assert.throws(() => rootVariablesFromSearch(items, "3"), /truncated/);
  // A truncated variable in a NESTED scope is filtered out first, so it must not
  // trip the guard for the root observation.
  const nested = [
    { name: "big", value: "zzz", processInstanceKey: "3", scopeKey: "42", isTruncated: true },
  ];
  assert.deepEqual(rootVariablesFromSearch(nested, "3"), {});
});

test("CamundaBackend.ping resolves false on a transport failure (skip case)", async () => {
  // An unreachable endpoint must be the skip case, not an exit-1 crash (#1260).
  const backend = new CamundaBackend({ address: "http://127.0.0.1:1" });
  assert.equal(await backend.ping(), false);
});

test("CamundaBackend.ping throws on an HTTP error (misconfigured runtime fails loudly)", async () => {
  const server = createServer((_req, res) => {
    res.writeHead(401, { "content-type": "application/json" });
    res.end('{"message":"unauthorized"}');
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    await assert.rejects(() => backend.ping(), /HTTP 401/);
  } finally {
    server.close();
    await once(server, "close");
  }
});
