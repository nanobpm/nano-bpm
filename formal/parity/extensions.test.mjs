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
import { classifyDeployFailure, probeModel } from "./runtime/extension-acceptance.mjs";
import { CamundaHttpError } from "./runtime/camunda-backend.mjs";

// A scratch directory holding a synthetic register + surface + coverage + corpus,
// so each rule can be tripped in isolation without touching the committed files.
function fixture({ extensions, surfaceCells = [], coverageRules = [], bpmn = {}, runtimeScenarios = {} }) {
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
  const runtimeCorpusDir = path.join(dir, "runtime-corpus");
  fs.mkdirSync(runtimeCorpusDir);
  for (const [name, { bpmnFile = "process.bpmn", xml }] of Object.entries(runtimeScenarios)) {
    const scenarioDir = path.join(runtimeCorpusDir, name);
    fs.mkdirSync(scenarioDir);
    fs.writeFileSync(path.join(scenarioDir, "scenario.json"), JSON.stringify({ name, bpmn: bpmnFile }));
    fs.writeFileSync(path.join(scenarioDir, bpmnFile), xml);
  }
  return {
    extensionsFile: path.join(dir, "extensions.json"),
    surfaceFile: path.join(dir, "zeebe-surface.json"),
    coverageFile: path.join(dir, "coverage.json"),
    graphsDir,
    bpmnDir,
    runtimeCorpusDir,
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

test("Red/Green: modelUsesExtension resolves default namespaces, shadowing and spaced `=`", () => {
  // Default-namespace form of the registered extension IS the extension.
  const dflt = `<bpmn:definitions xmlns:bpmn="b"><bpmn:extensionElements><cost xmlns="${NANO_NS}" value="1"/></bpmn:extensionElements></bpmn:definitions>`;
  assert.ok(modelUsesExtension(dflt, costEntry));
  // Whitespace around `=` in the prefix declaration.
  const spaced = `<bpmn:definitions xmlns:nano = "${NANO_NS}"><nano:cost value="1"/></bpmn:definitions>`;
  assert.ok(modelUsesExtension(spaced, costEntry));
  // A nested redeclaration shadows the outer binding: not the extension.
  const shadowed = `<d xmlns:nano="${NANO_NS}"><x xmlns:nano="http://other.example/"><nano:cost/></x></d>`;
  assert.ok(!modelUsesExtension(shadowed, costEntry));
  // A binding declared in a sibling subtree is out of scope.
  const sibling = `<d><a xmlns:nano="${NANO_NS}"/><nano:cost/></d>`;
  assert.ok(!modelUsesExtension(sibling, costEntry));
  // Unprefixed tag with no default namespace is not the extension.
  assert.ok(!modelUsesExtension(`<d><cost/></d>`, costEntry));
  // Commented-out usage does not count.
  assert.ok(!modelUsesExtension(`<d xmlns:nano="${NANO_NS}"><!-- <nano:cost/> --></d>`, costEntry));
});

test("Red/Green: an empty or whitespace-only xmlLocalName is rejected", () => {
  for (const local of ["", "   "]) {
    const f = fixture({
      extensions: [{ ...costEntry, match: { xmlLocalName: local, xmlNamespace: NANO_NS } }],
    });
    const problems = checkExtensions(f);
    assert.ok(problems.some((p) => p.includes("xmlLocalName must be a non-empty string")), `${JSON.stringify(local)}: ${problems}`);
  }
});

test("Red/Green: only an extension-specific 400 counts as a Camunda rejection", () => {
  const err = (status, body) => new CamundaHttpError("x", { status, body });
  assert.equal(classifyDeployFailure(costEntry, err(400, { detail: "unknown element 'cost'" })), "rejected");
  assert.equal(classifyDeployFailure(costEntry, err(400, { detail: "missing job type" })), "error");
  for (const s of [401, 403, 500, 503]) {
    assert.equal(classifyDeployFailure(costEntry, err(s, { detail: "cost" })), "error", `HTTP ${s}`);
  }
  assert.equal(classifyDeployFailure(costEntry, new Error("fetch failed")), "error");
  assert.equal(classifyDeployFailure(costEntry, Object.assign(new Error("timeout"), { name: "TimeoutError" })), "error");
});

test("Red/Green: a 400 naming the extension only via the generated process id is NOT a rejection", () => {
  const err = (body) => new CamundaHttpError("x", { status: 400, body });
  // The probe model embeds the local name in `probe_<local>` / `Defs_<local>`;
  // an unrelated validation failure citing the PROCESS id must not confirm the
  // extension (the name appears only in the process id).
  assert.equal(
    classifyDeployFailure(costEntry, err({ detail: "Process 'probe_cost': invalid process configuration" })),
    "error",
  );
  assert.equal(
    classifyDeployFailure(costEntry, err("Deployment of Defs_cost failed: no executable process")),
    "error",
  );
  // Extension-specific diagnostics DO count: the namespaced tag or the quoted
  // local name, even alongside the process id.
  assert.equal(
    classifyDeployFailure(costEntry, err({ detail: "Process 'probe_cost': unknown extension element nano:cost" })),
    "rejected",
  );
  assert.equal(
    classifyDeployFailure(costEntry, err({ detail: "Process 'probe_cost': unknown element 'cost' in extensionElements" })),
    "rejected",
  );
  assert.equal(
    classifyDeployFailure(costEntry, err({ detail: 'unsupported element "cost"' })),
    "rejected",
  );
});

test("Red/Green: probeModel places a sequenceFlow-only extension on a flow, others on the task", () => {
  const flowClassEntry = {
    ...costEntry,
    id: "bpmn:nano:flowClass",
    match: { xmlLocalName: "flowClass", xmlNamespace: NANO_NS, on: "sequenceFlow" },
  };
  const flowProbe = probeModel(flowClassEntry);
  assert.ok(flowProbe.includes(`<bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t">`), `flow probe opens the flow: ${flowProbe}`);
  assert.ok(flowProbe.includes("<nano:flowClass"), `flow probe carries the extension: ${flowProbe}`);
  assert.ok(!flowProbe.includes("<nano:flowClass probe=\"true\" />\n      </bpmn:extensionElements>\n    </bpmn:serviceTask"), "not on the task");
  // The default (no `match.on`) keeps the extension on the service task and
  // the flows self-closed.
  const taskProbe = probeModel(costEntry);
  assert.ok(taskProbe.includes("<nano:cost probe=\"true\" />"), `task probe carries the extension: ${taskProbe}`);
  assert.ok(taskProbe.includes(`<bpmn:sequenceFlow id="f1" sourceRef="s" targetRef="t" />`), `task probe's flow is self-closed: ${taskProbe}`);
});

test("the register covers every canonical nano-moddle annotation type", () => {
  // processos/assets/bpmn/nano-moddle.json is the canonical descriptor of the
  // `nano:*` annotation family the Workbench round-trips; every type it
  // declares must be registered (and therefore probed/scanned), or a model
  // using the unregistered type passes the guard invisibly.
  const moddle = JSON.parse(
    fs.readFileSync(path.join(path.dirname(EXTENSIONS_PATH), "..", "..", "processos", "assets", "bpmn", "nano-moddle.json"), "utf8"),
  );
  const registered = new Set(loadExtensions().map((e) => e.match?.xmlLocalName));
  for (const type of moddle.types ?? []) {
    const local = type.name.charAt(0).toLowerCase() + type.name.slice(1);
    assert.ok(registered.has(local), `nano-moddle type ${type.name} (nano:${local}) is not in the extension register`);
  }
  // And the sequence-flow-restricted type declares its probe placement.
  const flowClass = loadExtensions().find((e) => e.match?.xmlLocalName === "flowClass");
  assert.equal(flowClass?.match?.on, "sequenceFlow", "flowClass's descriptor restricts it to bpmn:SequenceFlow — the probe must place it there");
});

test("Red/Green: a wildcard coverage rule claims an extension id absent from the surface", () => {
  // `element:NanoOnlyWidget` is NOT a zeebe-surface cell; expanding `element:*`
  // over the surface alone would miss the overlap, letting the guard accept an
  // extension rule 2 must reject.
  const f = fixture({
    extensions: [{ ...costEntry, id: "element:NanoOnlyWidget" }],
    surfaceCells: ["element:ServiceTask"],
    coverageRules: [{ match: "element:*", status: "parity", evidence: ["traces:x"] }],
  });
  const problems = checkExtensions(f);
  assert.ok(
    problems.some((p) => p.includes("claimed by a coverage.json rule") && p.includes("element:NanoOnlyWidget")),
    `wildcard-over-extension-id must fail: ${problems}`,
  );
});

test("Red/Green: a runtime parity-corpus scenario using an extension fails", () => {
  const f = fixture({
    extensions: [costEntry],
    runtimeScenarios: { "nano-scenario": { xml: nanoModel } },
  });
  const problems = checkExtensions(f);
  assert.ok(
    problems.some((p) => p.includes("runtime parity-corpus scenario") && p.includes("nano-scenario")),
    `a runtime-corpus model using an extension must fail: ${problems}`,
  );
});

test("a runtime parity corpus with NO extension passes rule 4", () => {
  const f = fixture({
    extensions: [costEntry],
    runtimeScenarios: { "plain-scenario": { xml: plainModel } },
  });
  const problems = checkExtensions(f);
  assert.deepEqual(problems, [], `clean runtime corpus must pass: ${problems}`);
});

test("Red/Green: an unreadable runtime scenario.json fails closed (not a crash, not a skip)", () => {
  const f = fixture({ extensions: [costEntry] });
  const scenarioDir = path.join(f.runtimeCorpusDir, "broken");
  fs.mkdirSync(scenarioDir);
  fs.writeFileSync(path.join(scenarioDir, "scenario.json"), "{ not json");
  const problems = checkExtensions(f);
  assert.ok(
    problems.some((p) => p.includes("broken") && p.includes("fails closed")),
    `an unreadable scenario must fail closed: ${problems}`,
  );
});
