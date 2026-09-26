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
import { NanoBackend } from "./nano-backend.mjs";
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
