// Unit tests for the MCP processes view helpers (issue #1612).
import { test } from "node:test";
import assert from "node:assert/strict";
import { inputRows, normalizeCatalog } from "./mcpProcesses.ts";

test("inputRows lists declared inputs first and businessId last", () => {
  const rows = inputRows({
    type: "object",
    properties: {
      businessId: { type: "string", description: "reserved" },
      firstName: { type: "string", description: "The first name." },
      amount: { type: "integer" },
      blob: {},
    },
    required: ["amount"],
  });
  assert.deepEqual(
    rows.map((r) => [r.name, r.type, r.required, r.reserved]),
    [
      ["firstName", "string", false, false],
      ["amount", "integer", true, false],
      ["blob", "any", false, false],
      ["businessId", "string", false, true],
    ],
  );
  assert.equal(rows[0].description, "The first name.");
  assert.equal(rows[1].description, "");
});

test("inputRows flattens a nested data-envelope schema (#1738)", () => {
  const rows = inputRows({
    type: "object",
    properties: {
      businessId: { type: "string" },
      customer: {
        type: "object",
        properties: { email: { type: "string" } },
        required: ["email"],
      },
      lines: {
        type: "array",
        items: {
          type: "object",
          properties: { qty: { type: "integer" } },
          required: ["qty"],
        },
      },
      placed: { type: "string", format: "date-time" },
    },
    required: ["customer"],
  });
  assert.deepEqual(
    rows.map((r) => [r.name, r.type, r.required, r.reserved]),
    [
      ["customer", "object", true, false],
      ["customer.email", "string", true, false],
      ["lines", "array<object>", false, false],
      ["lines[].qty", "integer", true, false],
      ["placed", "string (date-time)", false, false],
      ["businessId", "string", false, true],
    ],
  );
});

test("inputRows quotes non-identifier names so distinct paths never collide", () => {
  const rows = inputRows({
    type: "object",
    properties: {
      "customer.email": { type: "string" },
      customer: {
        type: "object",
        properties: { email: { type: "string" }, "a[]": { type: "number" } },
      },
    },
  });
  const names = rows.map((r) => r.name);
  assert.deepEqual(names, [
    '["customer.email"]',
    "customer",
    "customer.email",
    'customer["a[]"]',
  ]);
  assert.equal(new Set(names).size, names.length);
});

test("inputRows tolerates a missing schema", () => {
  assert.deepEqual(inputRows(undefined), []);
  assert.deepEqual(inputRows({}), []);
});

test("normalizeCatalog fills defaults for a partial body", () => {
  const c = normalizeCatalog({});
  assert.equal(c.enabled, false);
  assert.equal(c.envVar, "NANOBPM_MCP_ENABLED");
  assert.equal(c.path, "/mcp/processes");
  assert.deepEqual(c.tools, []);
  assert.deepEqual(c.staticTools, []);
  assert.deepEqual(normalizeCatalog(null).tools, []);
});

test("normalizeCatalog passes a full body through", () => {
  const tool = {
    name: "t_1",
    title: "t",
    description: "d",
    inputSchema: {},
    toolName: "t",
    messageName: "m",
    subscriptionKey: "1",
    processDefinitionKey: "2",
    processDefinitionId: "p",
    processDefinitionVersion: 3,
  };
  const c = normalizeCatalog({
    enabled: true,
    envVar: "X",
    path: "/mcp/processes",
    staticTools: ["searchIncidents"],
    tools: [tool],
  });
  assert.equal(c.enabled, true);
  assert.deepEqual(c.tools, [tool]);
  assert.deepEqual(c.staticTools, ["searchIncidents"]);
});
