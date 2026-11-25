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
import { readFileSync } from "node:fs";
import { createServer } from "node:http";
import { once } from "node:events";
import {
  NanoBackend,
  incidentsFromSnapshot,
  rootVariablesFromSearch,
} from "./nano-backend.mjs";
import { CamundaBackend, BackendCleanupError } from "./camunda-backend.mjs";
import { runScenario } from "./driver.mjs";
import { discoverScenarios, installSignalCleanup, backendEligible } from "./run.mjs";
import { EventEmitter } from "node:events";
import {
  checkExpectations,
  diffObservations,
  emptyObservation,
  variablesFromSearchItems,
  MAX_JOBS_TO_ACTIVATE,
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

test("backendEligible honors a scenario's backends restriction (#1246 review)", () => {
  // The bounded-exhaustive corpus marks non-completing scenarios
  // `backends: ["nano"]`: the bare-gateway Camunda backend only observes an
  // instance through completion, so running one on Camunda would time out in
  // `awaitCompletion` even when both engines behave correctly. The shared
  // runner must skip it on camunda (and the differential) but still run nano.
  const nanoOnly = { name: "stuck", backends: ["nano"] };
  assert.equal(backendEligible(nanoOnly, "nano"), true);
  assert.equal(backendEligible(nanoOnly, "camunda"), false);
  // A scenario with no restriction runs on every backend.
  assert.equal(backendEligible({ name: "plain" }, "camunda"), true);
  assert.equal(backendEligible({ name: "plain" }, "nano"), true);
  // An explicit both-backends list is eligible on both.
  const both = { name: "both", backends: ["nano", "camunda"] };
  assert.equal(backendEligible(both, "camunda"), true);
  assert.equal(backendEligible(both, "nano"), true);
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
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Definitions_incident" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="parity-incident" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="GW" />
    <bpmn:exclusiveGateway id="GW"><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:exclusiveGateway>
    <bpmn:sequenceFlow id="f2" sourceRef="GW" targetRef="End">
      <bpmn:conditionExpression xsi:type="bpmn:tFormalExpression" xmlns:xsi="http://www.w3.org/2001/XMLSchema-instance">=missingVar = "x"</bpmn:conditionExpression>
    </bpmn:sequenceFlow>
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="parity-incident">
      <bpmndi:BPMNShape id="Start_di" bpmnElement="Start"><dc:Bounds x="152" y="182" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="GW_di" bpmnElement="GW" isMarkerVisible="true"><dc:Bounds x="245" y="175" width="50" height="50" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="End_di" bpmnElement="End"><dc:Bounds x="352" y="182" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1"><di:waypoint x="188" y="200" /><di:waypoint x="245" y="200" /></bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2"><di:waypoint x="295" y="200" /><di:waypoint x="352" y="200" /></bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
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
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Definitions_linear" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="parity-linear" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Task" />
    <bpmn:serviceTask id="Task"><bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Task" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="parity-linear">
      <bpmndi:BPMNShape id="Start_di" bpmnElement="Start"><dc:Bounds x="152" y="182" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Task_di" bpmnElement="Task"><dc:Bounds x="240" y="160" width="100" height="80" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="End_di" bpmnElement="End"><dc:Bounds x="392" y="182" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1"><di:waypoint x="188" y="200" /><di:waypoint x="240" y="200" /></bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2"><di:waypoint x="340" y="200" /><di:waypoint x="392" y="200" /></bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
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
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL" xmlns:bpmndi="http://www.omg.org/spec/BPMN/20100524/DI" xmlns:dc="http://www.omg.org/spec/DD/20100524/DC" xmlns:di="http://www.omg.org/spec/DD/20100524/DI" xmlns:zeebe="http://camunda.org/schema/zeebe/1.0" id="Definitions_linear_big" targetNamespace="http://bpmn.io/schema/bpmn">
  <bpmn:process id="parity-linear-big" isExecutable="true">
    <bpmn:startEvent id="Start"><bpmn:outgoing>f1</bpmn:outgoing></bpmn:startEvent>
    <bpmn:sequenceFlow id="f1" sourceRef="Start" targetRef="Task" />
    <bpmn:serviceTask id="Task"><bpmn:extensionElements><zeebe:taskDefinition type="work" /></bpmn:extensionElements><bpmn:incoming>f1</bpmn:incoming><bpmn:outgoing>f2</bpmn:outgoing></bpmn:serviceTask>
    <bpmn:sequenceFlow id="f2" sourceRef="Task" targetRef="End" />
    <bpmn:endEvent id="End"><bpmn:incoming>f2</bpmn:incoming></bpmn:endEvent>
  </bpmn:process>
  <bpmndi:BPMNDiagram id="BPMNDiagram_1">
    <bpmndi:BPMNPlane id="BPMNPlane_1" bpmnElement="parity-linear-big">
      <bpmndi:BPMNShape id="Start_di" bpmnElement="Start"><dc:Bounds x="152" y="182" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="Task_di" bpmnElement="Task"><dc:Bounds x="240" y="160" width="100" height="80" /></bpmndi:BPMNShape>
      <bpmndi:BPMNShape id="End_di" bpmnElement="End"><dc:Bounds x="392" y="182" width="36" height="36" /></bpmndi:BPMNShape>
      <bpmndi:BPMNEdge id="f1_di" bpmnElement="f1"><di:waypoint x="188" y="200" /><di:waypoint x="240" y="200" /></bpmndi:BPMNEdge>
      <bpmndi:BPMNEdge id="f2_di" bpmnElement="f2"><di:waypoint x="340" y="200" /><di:waypoint x="392" y="200" /></bpmndi:BPMNEdge>
    </bpmndi:BPMNPlane>
  </bpmndi:BPMNDiagram>
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

test("CamundaBackend.ping resolves false on a wedged endpoint that never responds (timeout skip)", async () => {
  // A reachable endpoint that accepts the connection but never answers must not
  // hang the skip-tolerant CI job forever — the bounded probe aborts on its
  // deadline and treats it as the same unreachable-runtime skip (#1260).
  const sockets = new Set();
  const server = createServer(() => {
    // Deliberately never write a response: hold the request open indefinitely.
  });
  server.on("connection", (s) => sockets.add(s));
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({
      address: `http://127.0.0.1:${port}`,
      pingTimeoutMs: 150,
    });
    assert.equal(await backend.ping(), false);
  } finally {
    for (const s of sockets) s.destroy();
    server.close();
    await once(server, "close");
  }
});

test("CamundaBackend normalises an address that already ends in /v2 (no /v2/v2)", () => {
  // `CAMUNDA_REST_ADDRESS` is documented as the full base ending in `/v2`, so
  // appending `/v2` unconditionally would target `/v2/v2` and miss the gateway.
  // Both the documented `/v2` form and a bare host must resolve to a single /v2.
  assert.equal(
    new CamundaBackend({ address: "http://localhost:8080/v2" }).base,
    "http://localhost:8080/v2",
  );
  assert.equal(
    new CamundaBackend({ address: "http://localhost:8080/v2/" }).base,
    "http://localhost:8080/v2",
  );
  assert.equal(
    new CamundaBackend({ address: "http://localhost:8080" }).base,
    "http://localhost:8080/v2",
  );
  assert.equal(
    new CamundaBackend({ address: "http://localhost:8080/" }).base,
    "http://localhost:8080/v2",
  );
});

test("CamundaBackend.observe defaults an absent processCompleted flag to completed", async () => {
  // A real Camunda v2 awaitCompletion 200 IS the completion signal and carries
  // no processCompleted field, so an ABSENT flag must default to completed —
  // otherwise every successful Camunda scenario reports a false `completed`
  // mismatch. An explicit `false` (a server that reports the flag) is preserved.
  const backend = new CamundaBackend({ address: "http://localhost:8080/v2" });
  const absent = await backend.observe({
    pending: Promise.resolve({ variables: { a: 1 } }),
  });
  assert.equal(absent.completed, true);
  assert.deepEqual(absent.variables, { a: 1 });
  const incomplete = await backend.observe({
    pending: Promise.resolve({ processCompleted: false, variables: { a: 2 } }),
  });
  assert.equal(incomplete.completed, false);
  assert.deepEqual(incomplete.variables, { a: 2 });
  const done = await backend.observe({
    pending: Promise.resolve({ processCompleted: true, variables: { a: 3 } }),
  });
  assert.equal(done.completed, true);
  assert.deepEqual(done.variables, { a: 3 });
});

test("CamundaBackend.observe strips route_* schedule variables like NanoBackend (#1246)", async () => {
  // The driver's routing schedule (`route_*`) is fed INTO both backends, not a
  // process outcome. NanoBackend.observe drops it; Camunda must too, or the
  // differential reports a false `variables` mismatch on every routed scenario.
  const backend = new CamundaBackend({ address: "http://localhost:8080/v2" });
  const obs = await backend.observe({
    pending: Promise.resolve({
      variables: { route_G1: [["f2"]], corr: "k", out: 1 },
    }),
  });
  assert.deepEqual(obs.variables, { corr: "k", out: 1 }, "route_* dropped, process variables kept");
});

test("CamundaBackend.reset rewinds the pinned clock and fails loudly on a reset error", async () => {
  // A swallowed /clock/reset could leave the global clock pinned while the
  // runner reports success, so cleanup must fail loudly, not be caught (#1260).
  let mode = "ok";
  const server = createServer((req, res) => {
    if (req.url.endsWith("/clock/reset")) {
      if (mode === "fail") {
        res.writeHead(500, { "content-type": "application/json" });
        res.end('{"message":"boom"}');
        return;
      }
      res.writeHead(204);
      res.end();
      return;
    }
    res.writeHead(404);
    res.end();
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    backend.pinnedClockMs = 5000;
    await backend.reset();
    assert.equal(backend.pinnedClockMs, null);
    mode = "fail";
    await assert.rejects(() => backend.reset(), /reset clock failed: HTTP 500/);
    // A cleanup failure is a BackendCleanupError so the runner ABORTS the phase
    // instead of recording a false divergence and continuing on a non-isolated
    // backend (#1246 review).
    await assert.rejects(() => backend.reset(), (err) => err instanceof BackendCleanupError);
  } finally {
    server.close();
    await once(server, "close");
  }
});

test("CamundaBackend bounds ordinary REST calls too — a wedged endpoint rejects, never hangs", async () => {
  // The `ping()` deadline alone is not enough: every #json REST call (clock ops,
  // deploy, correlation, completion) must also be bounded, or a reachable-but-
  // wedged endpoint that accepts the connection but never answers leaves fetch()
  // pending forever and hangs the skip-tolerant CI job (#1260).
  const sockets = new Set();
  const server = createServer(() => {
    // Accept the request but never respond: hold it open indefinitely.
  });
  server.on("connection", (s) => sockets.add(s));
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({
      address: `http://127.0.0.1:${port}`,
      requestTimeoutMs: 150,
    });
    backend.pinnedClockMs = 5000;
    // `reset()` issues a #json POST /clock/reset — it must reject on the deadline,
    // not hang. A swallowed reset is separately guarded elsewhere; here we only
    // assert the bounded request fails loudly rather than pending forever.
    await assert.rejects(() => backend.reset());
  } finally {
    for (const s of sockets) s.destroy();
    server.close();
    await once(server, "close");
  }
});

test("nano activateAndComplete fails when no job is activatable (parity with Camunda)", async () => {
  // The Camunda adapter throws when its long-poll yields no matching job; nano
  // must fail identically instead of silently returning 0, so a malformed or
  // regressed scenario cannot pass its oracle on a mismatched run (#1260).
  const nano = await new NanoBackend().init();
  try {
    await nano.deploy(INCIDENT_BPMN);
    const handle = await nano.start("parity-incident", {});
    await assert.rejects(
      () => nano.activateAndComplete(handle, "no-such-type", {}),
      /no 'no-such-type' job was activatable/,
    );
  } finally {
    await nano.close();
  }
});

test("job activation cap is shared, finite, and identical across backends", () => {
  // Both adapters must activate the same bounded number of matching jobs so a
  // model with many same-type jobs drives an identical step sequence; an
  // unbounded nano activation (MAX_SAFE_INTEGER) would diverge from Camunda.
  assert.equal(Number.isSafeInteger(MAX_JOBS_TO_ACTIVATE), true);
  assert.ok(MAX_JOBS_TO_ACTIVATE > 0 && MAX_JOBS_TO_ACTIVATE < Number.MAX_SAFE_INTEGER);
});

test("the native replay activation cap matches the runtime cap (no cross-runner drift)", () => {
  // engine-core/tests/parity_corpus.rs replays the same corpus natively for
  // LLVM coverage and must activate with the same cap, or a scenario with more
  // ready jobs of one type than the smaller cap completes a different number
  // of jobs per `activateAndComplete` step on each runner. Parse the constant
  // out of the Rust source so the two cannot drift apart silently.
  const rs = readFileSync(new URL("../../../engine-core/tests/parity_corpus.rs", import.meta.url), "utf8");
  const m = rs.match(/const MAX_JOBS_TO_ACTIVATE: usize = (\d+);/);
  assert.ok(m, "parity_corpus.rs no longer declares MAX_JOBS_TO_ACTIVATE");
  assert.equal(Number(m[1]), MAX_JOBS_TO_ACTIVATE);
});

test("a failed scenario aborts the in-flight awaitCompletion create (no hang until its deadline)", async () => {
  // If a step fails before `observe()`, the awaitCompletion create would stay in
  // flight for its full ~135 s deadline, keeping the CLI/CI job alive. Cleanup
  // (`reset`, which `runScenario` and `close` both reach) must abort it (#1260).
  let createReq = null;
  const server = createServer((req, res) => {
    if (req.url.endsWith("/process-instances")) {
      createReq = req; // never answered: the instance "never completes"
      return;
    }
    if (req.url.endsWith("/jobs/activation")) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"jobs":[]}');
      return;
    }
    res.writeHead(204);
    res.end();
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    const scenario = {
      xml: "<definitions/>",
      processId: "p",
      steps: [{ op: "activateAndComplete", jobType: "t" }],
    };
    await assert.rejects(() => runScenario(backend, scenario));
    const started = Date.now();
    await backend.close();
    // The create must have been aborted client-side: the server sees the socket close.
    if (!createReq.destroyed) await once(createReq, "close");
    assert.ok(Date.now() - started < 2000, "create was not aborted by cleanup");
  } finally {
    server.closeAllConnections();
    server.close();
    await once(server, "close");
  }
});

test("cleanup cancels a failed scenario's still-running Camunda instance", async () => {
  // Aborting the awaitCompletion create only frees the client: the instance keeps
  // running on the cluster, and its jobs could be taken by a later run that uses
  // the same job type. Cleanup must cancel every instance the scenario learned
  // the key of (from its activated jobs) (#1260 review).
  const cancelled = [];
  let activations = 0;
  const server = createServer((req, res) => {
    if (req.url.endsWith("/process-instances")) return; // never completes
    const m = req.url.match(/\/process-instances\/(\d+)\/cancellation$/);
    if (m) {
      cancelled.push(m[1]);
      res.writeHead(204);
      res.end();
      return;
    }
    if (req.url.endsWith("/jobs/activation")) {
      activations++;
      res.writeHead(200, { "content-type": "application/json" });
      res.end(
        activations === 1
          ? '{"jobs":[{"jobKey":"11","processInstanceKey":"4242"}]}'
          : '{"jobs":[]}',
      );
      return;
    }
    res.writeHead(204);
    res.end();
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    const scenario = {
      xml: "<definitions/>",
      processId: "p",
      steps: [
        { op: "activateAndComplete", jobType: "a" },
        { op: "activateAndComplete", jobType: "b" },
      ],
    };
    await assert.rejects(() => runScenario(backend, scenario));
    await backend.close();
    assert.deepEqual(cancelled, ["4242"]);
  } finally {
    server.closeAllConnections();
    server.close();
    await once(server, "close");
  }
});

test("a REJECTED create (awaitCompletion timeout) still cancels its instance on reset (#1246)", async () => {
  // Dropping the handle from #inflight on a create rejection would orphan the
  // still-running instance; a later scenario reusing its job type could activate
  // the leftover job. The handle must be retained until reset confirms the
  // cancellation of every key it learned.
  const cancelled = [];
  let activations = 0;
  const server = createServer((req, res) => {
    // The create responds 504 (awaitCompletion timeout) — the instance is
    // still running server-side, parked on its job.
    if (req.url.endsWith("/process-instances") && req.method === "POST") {
      res.writeHead(504, { "content-type": "application/json" });
      res.end('{"message":"timeout"}');
      return;
    }
    const m = req.url.match(/\/process-instances\/(\d+)\/cancellation$/);
    if (m) {
      cancelled.push(m[1]);
      res.writeHead(204);
      res.end();
      return;
    }
    if (req.url.endsWith("/jobs/activation")) {
      activations++;
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"jobs":[{"jobKey":"11","processInstanceKey":"777"}]}');
      return;
    }
    res.writeHead(204);
    res.end();
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    const scenario = {
      xml: "<definitions/>",
      processId: "p",
      steps: [{ op: "completeOneJob", jobType: "T1" }],
    };
    // The step completes a job (learning key 777), then observe() awaits the
    // rejected create and the scenario fails.
    await assert.rejects(() => runScenario(backend, scenario));
    await backend.close();
    assert.deepEqual(cancelled, ["777"], "the rejected create's instance must still be cancelled");
  } finally {
    server.closeAllConnections();
    server.close();
    await once(server, "close");
  }
});

test("completeOneJob activates and completes exactly ONE job of the type", async () => {
  const completions = [];
  const server = createServer((req, res) => {
    if (req.url.endsWith("/jobs/activation")) {
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"jobs":[{"jobKey":"21","processInstanceKey":"5"}]}');
      return;
    }
    const m = req.url.match(/\/jobs\/(\d+)\/completion$/);
    if (m) {
      completions.push(m[1]);
      res.writeHead(204);
      res.end();
      return;
    }
    res.writeHead(204);
    res.end();
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    const handle = { instanceKeys: new Set() };
    const n = await backend.completeOneJob(handle, "T1", {});
    assert.equal(n, 1);
    assert.deepEqual(completions, ["21"]);
    assert.deepEqual([...handle.instanceKeys], ["5"]);
    // No activatable job is a loud serialisation defect, like the batch op.
    const empty = createServer((req, res) => {
      if (req.url.endsWith("/jobs/activation")) {
        res.writeHead(200, { "content-type": "application/json" });
        res.end('{"jobs":[]}');
        return;
      }
      res.writeHead(204);
      res.end();
    });
    empty.listen(0);
    await once(empty, "listening");
    try {
      const b2 = new CamundaBackend({ address: `http://127.0.0.1:${empty.address().port}` });
      await assert.rejects(() => b2.completeOneJob({ instanceKeys: new Set() }, "T9", {}), /no 'T9' job became activatable/);
    } finally {
      empty.closeAllConnections();
      empty.close();
      await once(empty, "close");
    }
  } finally {
    server.closeAllConnections();
    server.close();
    await once(server, "close");
  }
});

test("publishMessage with a settled barrier long-polls the follow-on job and fails it back", async () => {
  // The correlation barrier (#1246 review): the publication endpoint does not
  // wait for correlation, so the driver must not advance while the message is
  // still buffered. With `settled.jobType` the publish long-polls one
  // activation of that type (the job exists only once the message correlated)
  // and fails it back so the driver's own completion step re-activates it.
  const calls = [];
  const server = createServer((req, res) => {
    if (req.url.endsWith("/messages/publication")) {
      calls.push("publish");
      res.writeHead(200, { "content-type": "application/json" });
      res.end("{}");
      return;
    }
    if (req.url.endsWith("/jobs/activation")) {
      calls.push("activate");
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"jobs":[{"jobKey":"31","processInstanceKey":"5"}]}');
      return;
    }
    const f = req.url.match(/\/jobs\/(\d+)\/failure$/);
    if (f) {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", () => {
        // JobFailRequest (spec/jobs.yaml) names the back-off `retryBackOff`
        // (additionalProperties:false) — capture the body so the assertion
        // below fails loudly if the driver sends the v1 `retryBackOffMs`.
        calls.push(`failback:${f[1]}:${body}`);
        res.writeHead(204);
        res.end();
      });
      return;
    }
    res.writeHead(204);
    res.end();
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    const handle = { instanceKeys: new Set() };
    await backend.publishMessage(handle, "M", "k", {}, { jobType: "T1" });
    assert.deepEqual(calls, [
      "publish",
      "activate",
      'failback:31:{"retries":1,"retryBackOff":0}',
    ]);
    assert.deepEqual([...handle.instanceKeys], ["5"]);
    // Without a barrier (catch leads straight to the end event) there is no
    // activation — the awaitCompletion create settling is the barrier.
    calls.length = 0;
    await backend.publishMessage(handle, "M", "k", {}, undefined);
    assert.deepEqual(calls, ["publish"]);
  } finally {
    server.closeAllConnections();
    server.close();
    await once(server, "close");
  }
});

test("setVariables targets the instance's root scope and is loud without a known key", async () => {
  const seen = [];
  const server = createServer((req, res) => {
    // Camunda v2 updates a scope's variables via
    // `PUT /element-instances/{elementInstanceKey}/variables` — the mock requires
    // BOTH the documented method and path so a wrong route fails loudly (#1246).
    const m = req.url.match(/\/element-instances\/(\d+)\/variables$/);
    if (m && req.method === "PUT") {
      let body = "";
      req.on("data", (c) => (body += c));
      req.on("end", () => {
        // The request body is a SetVariableRequest: `{ variables: {...} }`
        // (additionalProperties:false), NOT a flat variables object (#1246).
        seen.push({ key: m[1], body: JSON.parse(body || "{}") });
        res.writeHead(200, { "content-type": "application/json" });
        res.end("{}");
      });
      return;
    }
    res.writeHead(204);
    res.end();
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    const handle = { instanceKeys: new Set(["8080"]) };
    await backend.setVariables(handle, { route_G1: [["f2"]] });
    assert.deepEqual(seen, [{ key: "8080", body: { variables: { route_G1: [["f2"]] } } }]);
    await assert.rejects(
      () => backend.setVariables({ instanceKeys: new Set() }, { a: 1 }),
      /before the instance key is known/,
    );
  } finally {
    server.closeAllConnections();
    server.close();
    await once(server, "close");
  }
});

test("SIGINT/SIGTERM run backend cleanup (unpin the cluster clock) before exiting", async () => {
  // A killed run must not leave the cluster-global Camunda clock pinned at epoch
  // 0, freezing timers for every other user of the cluster (#1260 review).
  for (const sig of ["SIGINT", "SIGTERM"]) {
    const proc = new EventEmitter();
    const exits = [];
    proc.exit = (code) => exits.push(code);
    let cleaned = 0;
    installSignalCleanup(async () => {
      cleaned++;
    }, proc);
    proc.emit(sig);
    await new Promise((r) => setImmediate(r));
    assert.equal(cleaned, 1, `${sig} ran cleanup`);
    assert.deepEqual(exits, [sig === "SIGINT" ? 130 : 143]);
  }
});

test("signal cleanup still exits non-zero when cleanup itself fails", async () => {
  const proc = new EventEmitter();
  const exits = [];
  proc.exit = (code) => exits.push(code);
  installSignalCleanup(async () => {
    throw new Error("reset clock failed");
  }, proc);
  proc.emit("SIGTERM");
  await new Promise((r) => setImmediate(r));
  assert.deepEqual(exits, [143]);
});

test("--require-camunda fails (never skips) when no Camunda runtime is reachable", async () => {
  // The daily live-Camunda job exists only for the differential: a skip there
  // would be a green run that compared nothing (#1408). Without the flag the
  // runner stays skip-tolerant.
  const { spawnSync } = await import("node:child_process");
  const runner = fileURLToPath(new URL("./run.mjs", import.meta.url));
  const env = { ...process.env };
  delete env.CAMUNDA_REST_ADDRESS;
  const run = (args, extraEnv = {}) =>
    spawnSync(process.execPath, [runner, ...args], {
      env: { ...env, ...extraEnv },
      encoding: "utf8",
    });

  const unset = run(["--backend", "both", "--require-camunda"]);
  assert.notEqual(unset.status, 0, "unset address must fail");
  assert.match(unset.stderr, /Camunda backend required \(--require-camunda\)/);

  const unreachable = run(["--backend", "camunda", "--require-camunda"], {
    CAMUNDA_REST_ADDRESS: "http://127.0.0.1:9",
  });
  assert.notEqual(unreachable.status, 0, "unreachable runtime must fail");
  assert.match(unreachable.stderr, /Camunda backend required \(--require-camunda\)/);

  const tolerant = run(["--backend", "camunda"]);
  assert.equal(tolerant.status, 0, "without the flag the skip stays tolerant");
});

test("CamundaBackend settled.ack delivers the message ONCE, via synchronous correlation only (#1246 review)", async () => {
  // Publishing and then correlating would send a SECOND message: it 404s once
  // the first consumed the subscription, or leaves a stray buffered message.
  // The ack must be the delivery itself.
  const calls = [];
  const server = createServer((req, res) => {
    calls.push(req.url);
    req.resume();
    req.on("end", () => {
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"processInstanceKey":"1"}');
    });
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    await backend.publishMessage({ instanceKeys: new Set() }, "M", "k", {}, { ack: true });
    assert.equal(calls.filter((u) => u.endsWith("/messages/publication")).length, 0, "no buffered publication");
    assert.equal(calls.filter((u) => u.endsWith("/messages/correlation")).length, 1, "exactly one correlation");
  } finally {
    server.close();
    await once(server, "close");
  }
});

test("CamundaBackend settled.ack waits for a delayed subscription: a 404 correlation is re-attempted, never published (#1246 review)", async () => {
  // `/messages/correlation` is unbuffered: before the subscription opens it
  // answers 404 and delivers NOTHING, so re-attempting it is a readiness wait
  // (no second message), and the first 2xx is the single delivery + ack.
  const calls = [];
  let notReady = 2;
  const server = createServer((req, res) => {
    calls.push(req.url);
    req.resume();
    req.on("end", () => {
      if (req.url.endsWith("/messages/correlation") && notReady-- > 0) {
        res.writeHead(404, { "content-type": "application/json" });
        res.end('{"title":"NOT_FOUND"}');
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end('{"processInstanceKey":"1"}');
    });
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    await backend.publishMessage({ instanceKeys: new Set() }, "M", "k", {}, { ack: true });
    assert.equal(calls.filter((u) => u.endsWith("/messages/publication")).length, 0, "no buffered publication");
    assert.equal(calls.filter((u) => u.endsWith("/messages/correlation")).length, 3, "two not-ready 404s, then one delivery");
  } finally {
    server.close();
    await once(server, "close");
  }
});

test("CamundaBackend settled.ack fails loudly on a non-404 correlation error (#1246 review)", async () => {
  const calls = [];
  const server = createServer((req, res) => {
    calls.push(req.url);
    req.resume();
    req.on("end", () => {
      res.writeHead(400, { "content-type": "application/json" });
      res.end('{"title":"INVALID_ARGUMENT"}');
    });
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    await assert.rejects(
      backend.publishMessage({ instanceKeys: new Set() }, "M", "k", {}, { ack: true }),
      /HTTP 400/,
    );
    assert.equal(calls.length, 1, "a non-readiness error is never re-attempted");
  } finally {
    server.close();
    await once(server, "close");
  }
});

// #1918 review: the Camunda backend has no timer-readiness/settlement barrier
// and does not observe timer firings, so a hand-written corpus scenario that
// advances the clock must be nano-only (mirrors formal/corpus/enumerate.mjs).
test("every corpus scenario that advances time is nano-only", async () => {
  const { readdirSync, readFileSync, existsSync } = await import("node:fs");
  const dir = new URL("./corpus/", import.meta.url);
  for (const name of readdirSync(dir)) {
    const file = new URL(`${name}/scenario.json`, dir);
    if (!existsSync(file)) continue;
    const s = JSON.parse(readFileSync(file, "utf8"));
    if (!(s.steps ?? []).some((st) => st.op === "advanceTime")) continue;
    assert.deepEqual(s.backends, ["nano"], `${name} advances time but is not nano-only`);
  }
});

// #1969: a publication that carries an explicit timeToLive / messageId is sent
// AS GIVEN (it exercises Zeebe buffering itself), and `expectRejected` asserts
// the broker's 409 ALREADY_EXISTS for a still-buffered messageId.
test("publishMessage with timeToLive/messageId publishes as given and asserts a 409 rejection", async () => {
  const bodies = [];
  let reject = false;
  const server = createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      bodies.push(`${req.url.replace(/^.*\/messages/, "/messages")} ${body}`);
      if (reject) {
        res.writeHead(409, { "content-type": "application/json" });
        res.end('{"title":"ALREADY_EXISTS"}');
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end("{}");
    });
  });
  server.listen(0);
  await once(server, "listening");
  const { port } = server.address();
  try {
    const backend = new CamundaBackend({ address: `http://127.0.0.1:${port}` });
    const handle = { instanceKeys: new Set() };
    await backend.publishMessage(handle, "M", "k", { n: 1 }, undefined, {
      timeToLive: 0,
      messageId: "m1",
    });
    assert.deepEqual(bodies, [
      '/messages/publication {"name":"M","correlationKey":"k","variables":{"n":1},"timeToLive":0,"messageId":"m1"}',
    ]);
    // A rejection that was expected passes …
    reject = true;
    await backend.publishMessage(handle, "M", "k", {}, undefined, {
      timeToLive: 600000,
      messageId: "m1",
      expectRejected: true,
    });
    // … one that was not fails loudly, and so does an expected one that was accepted.
    await assert.rejects(
      backend.publishMessage(handle, "M", "k", {}, undefined, { timeToLive: 600000, messageId: "m1" }),
    );
    reject = false;
    await assert.rejects(
      backend.publishMessage(handle, "M", "k", {}, undefined, {
        timeToLive: 600000,
        messageId: "m1",
        expectRejected: true,
      }),
      /expected to be rejected/,
    );
    // A buffering publication takes no `settled` barrier (it would publish twice).
    await assert.rejects(
      backend.publishMessage(handle, "M", "k", {}, { ack: true }, { timeToLive: 1 }),
      /no `settled` barrier/,
    );
  } finally {
    server.closeAllConnections();
    server.close();
    await once(server, "close");
  }
});
