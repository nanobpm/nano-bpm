// Tests for the fail-closed manifest validator (ADR 0027 §4) — `node --test`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import { dirname, join } from "node:path";
import { buildSymbolIndex } from "../src/symbol-index.ts";
import { validateManifest } from "../src/validate.ts";

const here = dirname(fileURLToPath(import.meta.url));
const fx = (name) => readFileSync(join(here, "fixtures", name), "utf8");
const manifest = () => JSON.parse(fx("heating.nano.app.json"));

const models = [
  { path: "heating.bpmn", kind: "bpmn", text: fx("heating.bpmn") },
  { path: "triage.dmn", kind: "dmn", text: fx("triage.dmn") },
  { path: "confirm-heating.form", kind: "form", text: fx("confirm-heating.form") },
];

const codesFor = (result, pointer) =>
  result.diagnostics.filter((d) => d.pointer === pointer).map((d) => d.code);

test("a manifest whose references all resolve is valid", async () => {
  const index = await buildSymbolIndex(models);
  const result = validateManifest(manifest(), index);
  assert.deepEqual(result.diagnostics, []);
  assert.equal(result.ok, true);
});

test("schema errors fail closed with a JSON pointer, before cross-ref runs", () => {
  const m = manifest();
  delete m.id; // required
  m.triggers[0].action.start = 12345; // wrong type
  const result = validateManifest(m);
  assert.equal(result.ok, false);
  assert.ok(result.diagnostics.every((d) => d.code === "schema"));
  assert.ok(result.diagnostics.some((d) => d.pointer === "/id"));
});

test("network.bind accepts the loopback/all enum (nanobpm/nano-ide#235)", async () => {
  const index = await buildSymbolIndex(models);
  for (const bind of ["loopback", "all"]) {
    const m = manifest();
    m.network = { bind };
    const result = validateManifest(m, index);
    assert.deepEqual(result.diagnostics, [], `bind=${bind} should validate`);
    assert.equal(result.ok, true);
  }
});

test("network.bind rejects an out-of-enum value with a schema pointer", () => {
  const m = manifest();
  m.network = { bind: "public" }; // not a BindMode
  const result = validateManifest(m);
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/network/bind"), ["schema"]);
});

test("surfaces.pages.gridLayout accepts the auto/cards enum (nanobpm/nano-ide#572)", async () => {
  const index = await buildSymbolIndex(models);
  for (const gridLayout of ["auto", "cards"]) {
    const m = manifest();
    m.surfaces = { ...m.surfaces, pages: { enabled: true, gridLayout } };
    const result = validateManifest(m, index);
    assert.deepEqual(result.diagnostics, [], `gridLayout=${gridLayout} should validate`);
  }
});

test("surfaces.pages.gridLayout rejects an out-of-enum value with a schema pointer", () => {
  const m = manifest();
  m.surfaces = { ...m.surfaces, pages: { enabled: true, gridLayout: "table" } };
  const result = validateManifest(m);
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/surfaces/pages/gridLayout"), ["schema"]);
});

test("network block rejects unknown keys (additionalProperties: false)", () => {
  const m = manifest();
  m.network = { bind: "loopback", nope: true };
  const result = validateManifest(m);
  assert.equal(result.ok, false);
  assert.ok(result.diagnostics.every((d) => d.code === "schema"));
});

test("a manifest with no `models` key validates (deploy-by-convention, ADR 0062)", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  delete m.models;
  const result = validateManifest(m, index);
  assert.deepEqual(result.diagnostics, []);
  assert.equal(result.ok, true);
});

test("`models.templates` is rejected (removed by ADR 0062)", () => {
  const m = manifest();
  m.models = { ...m.models, templates: ["prompts/*.md"] };
  const result = validateManifest(m);
  assert.equal(result.ok, false);
  assert.ok(result.diagnostics.every((d) => d.code === "schema"));
  assert.ok(result.diagnostics.some((d) => d.pointer.startsWith("/models")));
});

test("`models.processes|decisions|forms` overrides still validate", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.models = {
    processes: ["processes/*.bpmn"],
    decisions: ["decisions/*.dmn"],
    forms: ["forms/*.form"],
  };
  const result = validateManifest(m, index);
  assert.deepEqual(result.diagnostics, []);
  assert.equal(result.ok, true);
});

test("unknown process / message / decision are rejected with pointers", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.triggers[0].action.start = "does-not-exist";
  m.triggers[1].action.message = "no-such-message";
  m.llm.classifier.output.decision = "missing-decision";
  const result = validateManifest(m, index);
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/triggers/0/action/start"), ["unknown-process"]);
  assert.deepEqual(codesFor(result, "/triggers/1/action/message"), ["unknown-message"]);
  assert.deepEqual(codesFor(result, "/llm/classifier/output/decision"), ["unknown-decision"]);
});

test("unknown connection, llm agent and datasource are rejected", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.triggers[1].auth = "hmac:ghost"; // connection "ghost" undeclared
  m.surfaces.chat.agent = "nobody"; // llm undeclared
  m.data.default = "elsewhere"; // datasource undeclared
  const result = validateManifest(m, index);
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/triggers/1/auth"), ["unknown-connection"]);
  assert.deepEqual(codesFor(result, "/surfaces/chat/agent"), ["unknown-llm"]);
  assert.deepEqual(codesFor(result, "/data/default"), ["unknown-datasource"]);
});

test("a pages surface sourceName must name a declared datasource (ADR 0027 §4)", () => {
  const m = manifest();
  m.surfaces.pages = { enabled: true, sourceName: "elsewhere" }; // datasource undeclared
  const result = validateManifest(m); // intra-manifest rule; no index needed
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/surfaces/pages/sourceName"), ["unknown-datasource"]);
});

test("manifest-only mode (no index) skips model rules but keeps intra-manifest rules", () => {
  const m = manifest();
  m.triggers[0].action.start = "does-not-exist"; // model rule — skipped without an index
  m.surfaces.chat.agent = "nobody"; // intra-manifest rule — still enforced
  const result = validateManifest(m); // no index
  assert.equal(result.ok, false);
  assert.equal(codesFor(result, "/triggers/0/action/start").length, 0);
  assert.deepEqual(codesFor(result, "/surfaces/chat/agent"), ["unknown-llm"]);
});

test("instanceTracking activeStatuses without statusField is incoherent", () => {
  const m = manifest();
  m.instanceTracking = [
    {
      table: "plans",
      keyField: "process_key",
      activeStatuses: ["planning"], // no statusField to read them from
      onTerminated: { set: { status: "abandoned" } },
    },
  ];
  const result = validateManifest(m); // intra-manifest rule; no index needed
  assert.equal(result.ok, false);
  assert.deepEqual(
    codesFor(result, "/instanceTracking/0/activeStatuses"),
    ["instance-tracking-incoherent"],
  );
});

test("instanceTracking with statusField + activeStatuses is coherent", () => {
  const m = manifest();
  m.instanceTracking = [
    {
      table: "plans",
      keyField: "process_key",
      statusField: "status",
      activeStatuses: ["planning"],
      onTerminated: { set: { status: "abandoned" } },
    },
  ];
  const result = validateManifest(m);
  assert.equal(result.ok, true);
  assert.deepEqual(codesFor(result, "/instanceTracking/0/activeStatuses"), []);
});

test("instanceTracking with statusField + terminalStatuses is coherent (fail-open selector)", () => {
  const m = manifest();
  m.instanceTracking = [
    {
      table: "feature_runs",
      keyField: "process_key",
      statusField: "status",
      terminalStatuses: ["merged", "abandoned"],
      onTerminated: { set: { status: "abandoned" } },
    },
  ];
  const result = validateManifest(m);
  assert.equal(result.ok, true);
  assert.deepEqual(codesFor(result, "/instanceTracking/0/terminalStatuses"), []);
});

test("instanceTracking terminalStatuses without statusField is incoherent", () => {
  const m = manifest();
  m.instanceTracking = [
    {
      table: "feature_runs",
      keyField: "process_key",
      terminalStatuses: ["merged"], // no statusField to read them from
      onTerminated: { set: { status: "abandoned" } },
    },
  ];
  const result = validateManifest(m); // intra-manifest rule; no index needed
  assert.equal(result.ok, false);
  assert.deepEqual(
    codesFor(result, "/instanceTracking/0/terminalStatuses"),
    ["instance-tracking-incoherent"],
  );
});

test("instanceTracking activeStatuses + terminalStatuses are mutually exclusive", () => {
  const m = manifest();
  m.instanceTracking = [
    {
      table: "feature_runs",
      keyField: "process_key",
      statusField: "status",
      activeStatuses: ["running"],
      terminalStatuses: ["merged"],
      onTerminated: { set: { status: "abandoned" } },
    },
  ];
  const result = validateManifest(m);
  assert.equal(result.ok, false);
  assert.deepEqual(
    codesFor(result, "/instanceTracking/0/terminalStatuses"),
    ["instance-tracking-incoherent"],
  );
});

// ── Domain type registry (ADR 0029 §4 / ADR 0031) ─────────────────────────────
import { resolveDomainTypes } from "../src/domain-types.ts";

test("a domain type whose field types are primitives or declared types is valid", () => {
  const m = manifest();
  m.types = {
    reading: { fields: { room: { type: "string" }, targetTemp: { type: "number" } } },
    schedule: { name: "Schedule", fields: { reading: { type: "reading", list: true } } },
  };
  const result = validateManifest(m); // intra-manifest rule, no index needed
  assert.deepEqual(
    result.diagnostics.filter((d) => d.pointer.startsWith("/types")),
    [],
  );
  assert.equal(result.ok, true);
});

test("a field type that is neither a primitive nor a declared type is rejected with a pointer", () => {
  const m = manifest();
  m.types = { reading: { fields: { room: { type: "strng" }, ref: { type: "no-such-type" } } } };
  const result = validateManifest(m);
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/types/reading/fields/room/type"), ["unknown-type"]);
  assert.deepEqual(codesFor(result, "/types/reading/fields/ref/type"), ["unknown-type"]);
});

test("a trigger bodyType must name a declared domain type (ADR 0029 §5)", () => {
  const m = manifest();
  m.types = { reading: { fields: { room: { type: "string" } } } };
  m.triggers[0].bodyType = "reading"; // resolves
  m.triggers[1] = { ...m.triggers[0], id: "other", bodyType: "no-such-type" }; // dangling
  const result = validateManifest(m); // intra-manifest rule, no index needed
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/triggers/0/bodyType"), []);
  assert.deepEqual(codesFor(result, "/triggers/1/bodyType"), ["unknown-type"]);
});

test("a FEEL body path outside the bodyType is flagged (ADR 0029 §5)", () => {
  const m = manifest();
  m.types = { reading: { fields: { room: { type: "string" } } } };
  m.triggers[0].bodyType = "reading";
  m.triggers[0].action = { start: "heating-cycle", variables: "= {r: body.room, x: body.nope}" };
  const result = validateManifest(m); // manifest-only: intra-manifest rule
  assert.deepEqual(codesFor(result, "/triggers/0/action/variables"), ["unknown-path"]);
});

test("a FEEL body path that resolves raises no diagnostic", () => {
  const m = manifest();
  m.types = { reading: { fields: { room: { type: "string" } } } };
  m.triggers[0].bodyType = "reading";
  m.triggers[0].action = { message: "temp-reading", correlationKey: "= body.room" };
  const result = validateManifest(m);
  assert.deepEqual(codesFor(result, "/triggers/0/action/correlationKey"), []);
});

test("resolveDomainTypes unions declared registry types with form-inferred candidates", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.types = { reading: { fields: { room: { type: "string" } } } };
  const resolution = resolveDomainTypes(m, index);

  assert.deepEqual(
    resolution.declared.map((d) => ({ id: d.id, match: d.match })),
    [{ id: "reading", match: "nominal" }],
  );
  // confirm-heating form inference remains a candidate (not shadowed by a declared type)
  assert.ok(resolution.inferred.some((r) => r.id === "confirm-heating"));
});

test("a declared type shadows its form-inferred candidate of the same id", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.types = { "confirm-heating": { fields: { room: { type: "string" } } } };
  const resolution = resolveDomainTypes(m, index);
  assert.ok(!resolution.inferred.some((r) => r.id === "confirm-heating"));
});

test("bindings to a resolvable form/decision + declared type are valid (ADR 0029 §5)", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.types = { ...(m.types ?? {}), reading: { fields: { room: { type: "string" } } } };
  m.bindings = [
    { form: "confirm-heating", type: "reading" },
    { decision: "email-triage", type: "reading" },
    { process: "heating-cycle", type: "reading" },
  ];
  const result = validateManifest(m, index);
  assert.deepEqual(codesFor(result, "/bindings/0/form"), []);
  assert.deepEqual(codesFor(result, "/bindings/1/decision"), []);
  assert.deepEqual(codesFor(result, "/bindings/2/process"), []);
  assert.deepEqual(codesFor(result, "/bindings/0/type"), []);
});

test("a binding to an unknown form/decision/type is rejected with pointers", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.bindings = [
    { form: "no-such-form", type: "reading" },
    { decision: "no-such-decision", type: "reading" },
    { process: "no-such-process", type: "reading" },
  ];
  const result = validateManifest(m, index);
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/bindings/0/form"), ["unknown-form"]);
  assert.deepEqual(codesFor(result, "/bindings/1/decision"), ["unknown-decision"]);
  assert.deepEqual(codesFor(result, "/bindings/2/process"), ["unknown-process"]);
  // "reading" is not declared here → all bindings flag the type.
  assert.deepEqual(codesFor(result, "/bindings/0/type"), ["unknown-type"]);
  assert.deepEqual(codesFor(result, "/bindings/1/type"), ["unknown-type"]);
});

test("a worker outputType must name a declared domain type (ADR 0033 §3)", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.types = { ...(m.types ?? {}), reading: { fields: { room: { type: "string" } } } };
  // fixture workers[0]=read-thermostat (handler), [1]=classify (llm)
  m.workers[0].outputType = "reading"; // declared → valid
  m.workers[1].outputType = "ghost"; // undeclared → unknown-type
  const result = validateManifest(m, index);
  assert.deepEqual(codesFor(result, "/workers/0/outputType"), []);
  assert.deepEqual(codesFor(result, "/workers/1/outputType"), ["unknown-type"]);
});

test("a worker inputType must name a declared domain type (ADR 0033 §3)", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  m.types = { ...(m.types ?? {}), reading: { fields: { room: { type: "string" } } } };
  // fixture workers[0]=read-thermostat (handler), [1]=classify (llm)
  m.workers[0].inputType = "reading"; // declared → valid
  m.workers[1].inputType = "ghost"; // undeclared → unknown-type
  const result = validateManifest(m, index);
  assert.deepEqual(codesFor(result, "/workers/0/inputType"), []);
  assert.deepEqual(codesFor(result, "/workers/1/inputType"), ["unknown-type"]);
});

test("a task type declared external must not also be wired in workers[]", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  // "read-thermostat" is app-hosted in workers[]; declaring it external too is contradictory,
  // while a genuinely external type (serviced by an out-of-process fleet) is fine.
  m.externalTaskTypes = ["read-thermostat", "senior:review"];
  const result = validateManifest(m, index);
  assert.deepEqual(codesFor(result, "/externalTaskTypes/0"), ["external-task-conflict"]);
  assert.deepEqual(codesFor(result, "/externalTaskTypes/1"), []);
});

test("a form field's datasource binding must name a declared source (ADR 0024 §5)", async () => {
  const boundForm = JSON.stringify({
    id: "orders",
    schemaVersion: 18,
    type: "default",
    components: [
      {
        type: "select",
        key: "customerId",
        id: "Field_1",
        dataSource: { source: "app", query: "SELECT id AS value, name AS label FROM customers" },
      },
      {
        type: "checklist",
        key: "regionIds",
        id: "Field_2",
        dataSource: { source: "warehouse", query: "SELECT id, name FROM regions" },
      },
    ],
  });
  const index = await buildSymbolIndex([
    ...models,
    { path: "orders.form", kind: "form", text: boundForm },
  ]);
  const result = validateManifest(manifest(), index);
  // "app" is declared in data.sources → no diagnostic; "warehouse" is not.
  assert.deepEqual(codesFor(result, "/forms/orders/fields/customerId/dataSource/source"), []);
  assert.deepEqual(
    codesFor(result, "/forms/orders/fields/regionIds/dataSource/source"),
    ["unknown-datasource"],
  );
  assert.equal(result.ok, false);
});

test("data.query in a trigger action must name a declared datasource (ADR 0024 §5)", async () => {
  const index = await buildSymbolIndex(models);
  const m = manifest();
  // Unknown alias → diagnostic; the default-source form and a declared alias are fine.
  m.triggers[1].action.correlationKey = '= data.query("ghost", "SELECT room FROM t")';
  m.triggers[0].action.variables = '= { seed: data.query("SELECT 1"), ok: data.query("app", "SELECT 1") }';
  const result = validateManifest(m, index);
  assert.equal(result.ok, false);
  assert.deepEqual(codesFor(result, "/triggers/1/action/correlationKey"), ["unknown-datasource"]);
  // default-source + declared "app" alias → no diagnostics on triggers[0].
  assert.deepEqual(codesFor(result, "/triggers/0/action/variables"), []);
});

test("data.query alias validation is intra-manifest (runs without an index)", () => {
  const m = manifest();
  m.triggers[1].action.correlationKey = '= data.query("ghost", "SELECT 1")';
  const result = validateManifest(m); // no index
  assert.deepEqual(codesFor(result, "/triggers/1/action/correlationKey"), ["unknown-datasource"]);
});
