/// <reference path="../element-templates-validator.d.ts" />
// Guards the built-in "MCP start event" component (#1611): schema validity,
// exact Camunda bindings, optional typed-input groups that emit nothing when
// empty, the dropdown type list vs. the server's JSON_SCHEMA_TYPES, and the
// shared BPMN fixture the server deploys under `/mcp/processes`.
import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { validateZeebe } from "@bpmn-io/element-templates-validator";
import {
  BUILTIN_COMPONENTS,
  MCP_INPUT_JSON_SCHEMA_TYPES,
  MCP_TOOL_PROPERTY_PREFIX,
  MCP_TYPED_INPUT_GROUPS,
  mcpStartEvent,
} from "./builtinComponents.ts";
import { combineComponents } from "./projectComponents.ts";
import type { ElementTemplate } from "./urbanComponents.ts";

const repoFile = (rel: string) =>
  readFileSync(new URL(`../../../${rel}`, import.meta.url), "utf8");

type Prop = Record<string, unknown> & {
  binding: { type: string; name?: string };
  optional?: boolean;
  value?: string;
};
const props = mcpStartEvent.properties as Prop[];
const zeebeProps = props.filter((p) => p.binding.type === "zeebe:property");

/** The `zeebe:property` entries bpmn-js-element-templates persists for the
 *  given field values: a non-optional binding always persists (empty if
 *  unset); an optional one only when non-empty (`shouldUpdate`). */
function emitted(values: Record<string, string>): Map<string, string> {
  const out = new Map<string, string>();
  for (const p of zeebeProps) {
    const name = p.binding.name as string;
    const value = values[name] ?? p.value ?? "";
    if (value || !p.optional) out.set(name, value);
  }
  return out;
}

test("every built-in validates against the Zeebe element-template schema", () => {
  for (const tpl of BUILTIN_COMPONENTS) {
    const { valid, errors } = validateZeebe(tpl);
    assert.ok(valid, `${tpl.id}: ${JSON.stringify(errors)}`);
  }
});

test("keeps Camunda's id and every io.camunda.tool binding exactly", () => {
  assert.equal(mcpStartEvent.id, "io.camunda.mcp.start-message");
  assert.deepEqual(mcpStartEvent.appliesTo, ["bpmn:StartEvent"]);
  assert.deepEqual(mcpStartEvent.elementType, {
    value: "bpmn:StartEvent",
    eventDefinition: "bpmn:MessageEventDefinition",
  });
  const camunda = zeebeProps.filter((p) => !p.optional);
  assert.deepEqual(
    camunda.map((p) => p.binding.name),
    [
      "name",
      "purpose",
      "inputs",
      "when_to_use",
      "when_not_to_use",
      "results",
    ].map((s) => `${MCP_TOOL_PROPERTY_PREFIX}${s}`),
  );
});

test("typed-input groups are bounded, optional and default-free", () => {
  const typed = zeebeProps.filter((p) =>
    (p.binding.name as string).startsWith(`${MCP_TOOL_PROPERTY_PREFIX}input_`),
  );
  assert.equal(typed.length, MCP_TYPED_INPUT_GROUPS * 4);
  for (const p of typed) {
    assert.equal(p.optional, true, String(p.binding.name));
    assert.equal(p.value, undefined, String(p.binding.name));
  }
  // Applying the template with nothing filled emits only Camunda's fields.
  assert.deepEqual(
    [...emitted({}).keys()].filter((k) => k.includes(":input_")),
    [],
  );
});

test("the Type dropdown offers exactly the server's JSON_SCHEMA_TYPES", () => {
  const rust = repoFile("server/src/mcp/processes_tools.rs");
  const m = /const JSON_SCHEMA_TYPES: \[&str; \d+\] = \[([^\]]*)\]/.exec(rust);
  assert.ok(m, "JSON_SCHEMA_TYPES not found in processes_tools.rs");
  const serverTypes = [...m[1].matchAll(/"([^"]+)"/g)].map((x) => x[1]);
  assert.deepEqual([...MCP_INPUT_JSON_SCHEMA_TYPES], serverTypes);
  const typeProp = props.find(
    (p) => p.binding.name === `${MCP_TOOL_PROPERTY_PREFIX}input_1_type`,
  );
  assert.deepEqual(
    (typeProp?.choices as Array<{ value: string }>).map((c) => c.value),
    serverTypes,
  );
});

test("the shared server fixture is exactly what the template emits", () => {
  const xml = repoFile(
    "engine-core/tests/fixtures/mcp-start-event-template.bpmn",
  );
  assert.match(xml, /<bpmndi:BPMNDiagram/, "fixture needs DI");
  assert.match(xml, /zeebe:modelerTemplate="io\.camunda\.mcp\.start-message"/);
  const inFixture = new Map(
    [...xml.matchAll(/<zeebe:property name="([^"]+)" value="([^"]*)"/g)].map(
      (x) => [x[1], x[2].replace(/&apos;/g, "'")] as [string, string],
    ),
  );
  const p = (s: string) => `${MCP_TOOL_PROPERTY_PREFIX}${s}`;
  const expected = emitted({
    [p("name")]: "lookup_customer",
    [p("purpose")]: "Look up a customer record.",
    [p("inputs")]: "A customer email and an optional limit.",
    [p("when_to_use")]: "The user asks about a customer.",
    [p("input_1_name")]: "email",
    [p("input_1_type")]: "string",
    [p("input_1_description")]: "The customer's email.",
    [p("input_1_required")]: "true",
    [p("input_2_name")]: "limit",
    [p("input_2_type")]: "integer",
  });
  assert.deepEqual(
    [...inFixture.entries()].sort(),
    [...expected.entries()].sort(),
  );
});

test("built-ins are always offered and lowest precedence", () => {
  assert.ok(combineComponents([], []).some((t) => t.id === mcpStartEvent.id));
  const override: ElementTemplate = {
    id: mcpStartEvent.id,
    name: "Project MCP",
    appliesTo: ["bpmn:StartEvent"],
    properties: [],
  };
  const merged = combineComponents([], [override]);
  assert.equal(merged.filter((t) => t.id === mcpStartEvent.id).length, 1);
  assert.equal(
    merged.find((t) => t.id === mcpStartEvent.id)?.name,
    "Project MCP",
  );
});
