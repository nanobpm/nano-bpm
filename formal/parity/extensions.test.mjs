// Tests for the extension-register guard (#1261, slice 6 of #1240).
//
// The register lists the Nano-only surface (what Camunda rejects or lacks); the
// guard fails when (a) an entry is malformed, (b) an entry collides with the
// Camunda surface (a difference there is a DEFECT, not an extension), or (c) a
// parity-checked corpus model is tagged as an extension. Each rule is proven to
// FIRE (Red/Green) against a synthetic fixture, and the REAL committed register
// is asserted clean.

import { test } from "node:test";
import assert from "node:assert/strict";
import fs from "node:fs";
import os from "node:os";
import path from "node:path";
import {
  checkExtensions,
  loadExtensions,
  modelUsesExtension,
  globToRegExp,
  EXTENSIONS_PATH,
} from "./extensions.mjs";

// A scratch directory holding a synthetic register + surface + coverage + corpus,
// so each rule can be tripped in isolation without touching the committed files.
function fixture({ extensions, surfaceCells = [], coverageRules = [], bpmn = {} }) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), "nano-ext-"));
  const write = (name, obj) =>
    fs.writeFileSync(path.join(dir, name), typeof obj === "string" ? obj : JSON.stringify(obj));
  write("extensions.json", { extensions });
  write("zeebe-surface.json", { zeebe: {}, cells: surfaceCells.map((id) => ({ id, sources: [] })) });
  write("coverage.json", { rules: coverageRules });
  const graphsDir = path.join(dir, "graphs");
  const bpmnDir = path.join(dir, "bpmn");
  fs.mkdirSync(graphsDir);
  fs.mkdirSync(bpmnDir);
  for (const [id, xml] of Object.entries(bpmn)) {
    fs.writeFileSync(path.join(bpmnDir, `${id}.bpmn`), xml);
  }
  return {
    extensionsFile: path.join(dir, "extensions.json"),
    surfaceFile: path.join(dir, "zeebe-surface.json"),
    coverageFile: path.join(dir, "coverage.json"),
    graphsDir,
    bpmnDir,
  };
}

const NANO_NS = "http://nano.camunda.io/schema/semantic/1.0";
const costEntry = {
  id: "bpmn:nano:cost",
  kind: "bpmn-attribute",
  issue: 1240,
  note: "per-element cost annotation",
  match: { xmlLocalName: "cost", xmlNamespace: NANO_NS },
};
const nanoModel = `<bpmn:definitions xmlns:nano="${NANO_NS}"><bpmn:process><bpmn:serviceTask id="t"><bpmn:extensionElements><nano:cost value="1"/></bpmn:extensionElements></bpmn:serviceTask></bpmn:process></bpmn:definitions>`;
const plainModel = `<bpmn:definitions><bpmn:process><bpmn:serviceTask id="t"/></bpmn:process></bpmn:definitions>`;

test("the committed register is well-formed and clean", () => {
  const problems = checkExtensions();
  assert.deepEqual(problems, [], `committed register must pass its own guard: ${problems.join("; ")}`);
  // And it must be non-empty — a register that lists nothing is not guarding
  // the Nano-only surface, it is an empty file.
  assert.ok(loadExtensions().length > 0, "the register must list the Nano-only surface");
  // Every entry carries a filed issue (the register never lists an untracked
  // difference).
  for (const e of loadExtensions()) {
    assert.ok(Number.isInteger(e.issue) && e.issue > 0, `${e.id} needs an issue`);
  }
});

test("globToRegExp: `*` matches any run, everything else is literal", () => {
  assert.ok(globToRegExp("element:*").test("element:ServiceTask"));
  assert.ok(!globToRegExp("element:*").test("intent:JOB:CREATED"));
  assert.ok(globToRegExp("failure:A.b:CONDITION_ERROR").test("failure:A.b:CONDITION_ERROR"));
  assert.ok(!globToRegExp("failure:A.b:CONDITION_ERROR").test("failure:A.b:IO_MAPPING_ERROR"));
});

test("modelUsesExtension is namespace-aware", () => {
  assert.ok(modelUsesExtension(nanoModel, costEntry));
  assert.ok(!modelUsesExtension(plainModel, costEntry));
  // A same-named tag bound to a DIFFERENT namespace is not the extension.
  const otherNs = `<bpmn:definitions xmlns:nano="http://other.example/"><nano:cost value="1"/></bpmn:definitions>`;
  assert.ok(!modelUsesExtension(otherNs, costEntry));
});

test("Red/Green: a malformed entry fails (no issue / no note / duplicate id)", () => {
  const f = fixture({
    extensions: [
      { id: "bpmn:nano:x", kind: "bpmn-attribute", note: "no issue", match: { xmlLocalName: "x" } },
      { id: "bpmn:nano:y", kind: "bpmn-attribute", issue: 1, match: { xmlLocalName: "y" } },
      costEntry,
      costEntry, // duplicate id
    ],
  });
  const problems = checkExtensions(f);
  assert.ok(problems.some((p) => p.includes("needs a filed issue")), `missing-issue: ${problems}`);
  assert.ok(problems.some((p) => p.includes("needs a note")), `missing-note: ${problems}`);
  assert.ok(problems.some((p) => p.includes("duplicate id")), `duplicate: ${problems}`);
});

test("Red/Green: an extension that collides with a Zeebe-surface cell fails", () => {
  // `element:ServiceTask` IS on the Camunda surface — listing it as a Nano
  // extension would define away a behavioural difference, which the register
  // must never do.
  const f = fixture({
    extensions: [{ ...costEntry, id: "element:ServiceTask" }],
    surfaceCells: ["element:ServiceTask"],
    coverageRules: [{ match: "element:ServiceTask", status: "parity", evidence: ["traces:x"] }],
  });
  const problems = checkExtensions(f);
  assert.ok(
    problems.some((p) => p.includes("collides with a Zeebe-surface cell")),
    `surface collision must fail: ${problems}`,
  );
  assert.ok(
    problems.some((p) => p.includes("claimed by a coverage.json rule")),
    `coverage claim must fail: ${problems}`,
  );
});

test("Red/Green: an extension claimed by a coverage rule (even out-of-scope) fails", () => {
  // A wildcard coverage rule claiming `element:*` sweeps up the extension id —
  // the surface is Zeebe's, not a Nano extension.
  const f = fixture({
    extensions: [{ ...costEntry, id: "element:NanoOnlyWidget" }],
    surfaceCells: ["element:NanoOnlyWidget"],
    coverageRules: [{ match: "element:*", status: "gap", issue: 1, note: "x" }],
  });
  const problems = checkExtensions(f);
  assert.ok(problems.some((p) => p.includes("claimed by a coverage.json rule")), problems.join("; "));
});

test("Red/Green: a parity-checked corpus model tagged as an extension fails", () => {
  const f = fixture({ extensions: [costEntry], bpmn: { SomeGraph: nanoModel } });
  const problems = checkExtensions(f);
  assert.ok(
    problems.some((p) => p.includes("parity-checked corpus model") && p.includes("SomeGraph")),
    `a corpus model using an extension must fail: ${problems}`,
  );
});

test("a parity corpus with NO extension passes rule 3", () => {
  const f = fixture({ extensions: [costEntry], bpmn: { SomeGraph: plainModel } });
  const problems = checkExtensions(f);
  assert.deepEqual(problems, [], `clean corpus must pass: ${problems}`);
});

test("Red/Green: an extension also named in the divergence allowlist fails (reconciliation)", () => {
  // The register and the allowlist are SEPARATE lists: an extension is a
  // Nano-only surface, an allowlist entry a DEFECT on the shared surface. The
  // same `nano:cost` subject in both masks a defect as an extension.
  const f = fixture({ extensions: [costEntry] });
  fs.writeFileSync(
    path.join(path.dirname(f.extensionsFile), "allowlist.json"),
    JSON.stringify({ entries: [{ scenario: "x", issue: "#1", reason: "nano:cost diverges" }] }),
  );
  const problems = checkExtensions({ ...f, allowlistFile: path.join(path.dirname(f.extensionsFile), "allowlist.json") });
  assert.ok(
    problems.some((p) => p.includes("also named in the divergence allowlist")),
    `register/allowlist overlap must fail: ${problems}`,
  );
});
