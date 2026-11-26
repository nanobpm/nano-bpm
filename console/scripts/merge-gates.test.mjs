import assert from "node:assert/strict";
import { test } from "node:test";
import {
  assertBranchProtection,
  assertLocalGates,
  readMergeGates,
  requiredStatusChecks,
} from "./merge-gates.mjs";

const config = readMergeGates();
const { protocol } = config;

test("required check names cannot collide with Mergify app qualification", () => {
  for (const { name } of protocol.requiredChecks) {
    assert.ok(
      !name.startsWith("@"),
      `${name} is interpreted as @app/check by Mergify`,
    );
  }
});

test("CI and Mergify gates agree with the canonical protocol", () => {
  assertLocalGates(config);
});

test("guard rejects required-job renames, duplicates and missing jobs", () => {
  for (const change of ["rename", "duplicate", "remove"]) {
    const copy = structuredClone(config);
    const name = protocol.requiredChecks[0].name;
    const id = Object.keys(copy.workflow.jobs).find(
      (id) => copy.workflow.jobs[id].name === name,
    );
    assert.ok(id, `Fixture must contain the required CI job ${name}`);
    if (change === "rename") copy.workflow.jobs[id].name += " renamed";
    if (change === "duplicate")
      copy.workflow.jobs.duplicate = copy.workflow.jobs[id];
    if (change === "remove") delete copy.workflow.jobs[id];
    assert.throws(() => assertLocalGates(copy), /exactly one CI job/);
  }
});

test("guard rejects missing Mergify gates or lost skip tolerance", () => {
  for (const change of ["missing", "skip"]) {
    const copy = structuredClone(config);
    const gate = copy.mergify.merge_protections.find(
      ({ name }) => name === "required CI checks",
    );
    assert.ok(gate, "Fixture must contain the required CI checks gate");
    if (change === "missing") gate.success_conditions.pop();
    if (change === "skip") gate.success_conditions[0].or.pop();
    assert.throws(
      () => assertLocalGates(copy),
      /Mergify required checks must match/,
    );
  }
});

test("branch protection update derives all checks and preserves strictness", () => {
  for (const strict of [true, false]) {
    const body = requiredStatusChecks(protocol, { strict });
    assert.equal(body.strict, strict);
    assert.deepEqual(
      body.checks.map(({ context }) => context),
      protocol.requiredChecks.map(({ name }) => name),
    );
    assert.ok(body.checks.every(({ app_id }) => app_id === 15368));
    assertBranchProtection(protocol, {
      ...body,
      checks: [...body.checks].reverse(),
    });
  }
});

test("live drift guard rejects missing console gates, renamed checks and wrong publishers", () => {
  for (const change of ["console", "rename", "publisher", "extra"]) {
    const current = requiredStatusChecks(protocol, { strict: false });
    if (change === "console")
      current.checks = current.checks.filter(
        ({ context }) => !context.startsWith("console "),
      );
    if (change === "rename") current.checks[0].context += " renamed";
    if (change === "publisher") current.checks[0].app_id = -1;
    if (change === "extra")
      current.checks.push({ context: "unexpected", app_id: 15368 });
    assert.throws(
      () => assertBranchProtection(protocol, current),
      /Live branch protection must match/,
    );
  }
});

test("live drift guard diagnoses a missing or malformed checks array", () => {
  for (const checks of [undefined, null, {}]) {
    assert.throws(
      () => assertBranchProtection(protocol, { strict: false, checks }),
      /Branch protection response must contain a checks array/,
    );
  }
});

test("parity-coverage ratchet is a required merge gate (#1977)", () => {
  // #1944 merged with `parity-coverage (nano llvm-cov)` red because the job
  // was in neither branch protection nor the Mergify gate.
  assert.ok(
    protocol.requiredChecks.some(
      ({ name }) => name === "parity-coverage (nano llvm-cov)",
    ),
    "parity-coverage (nano llvm-cov) must be a required check",
  );
});
