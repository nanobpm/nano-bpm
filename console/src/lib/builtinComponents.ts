// Built-in components — element templates the console ships to EVERY project,
// independent of installed packs or project files (ADR 0033 §2/§4). They sit at
// the lowest precedence (`combineComponents`): a pack or the project can shadow
// one by reusing its template id.

import { ELEMENT_TEMPLATE_SCHEMA_URL } from "./elementTemplateSchema";
import type { ElementTemplate } from "./urbanComponents";

/** The Camunda MCP-tool property namespace read by `/mcp/processes`
 *  (`server/src/mcp/processes_tools.rs`, `TOOL_PROPERTY_PREFIX`). */
export const MCP_TOOL_PROPERTY_PREFIX = "io.camunda.tool:";

/** JSON-Schema primitive types `build_input_schema` advertises for a typed
 *  input (`JSON_SCHEMA_TYPES` in `server/src/mcp/processes_tools.rs`). TS cannot
 *  import the Rust const, so `builtinComponents.test.ts` parses it from the Rust
 *  source and fails on any drift. */
export const MCP_INPUT_JSON_SCHEMA_TYPES = [
  "string",
  "integer",
  "number",
  "boolean",
  "object",
  "array",
  "null",
] as const;

/** Number of optional typed-input groups the MCP start event offers. */
export const MCP_TYPED_INPUT_GROUPS = 5;

const toolProperty = (suffix: string) => ({
  type: "zeebe:property",
  name: `${MCP_TOOL_PROPERTY_PREFIX}${suffix}`,
});

/** One optional `input_N_{name,type,description,required}` group (Nano superset
 *  of Camunda's tool schema). Every field is `optional`, so an unfilled group
 *  persists NO `zeebe:property` — the model stays valid on Camunda, which
 *  ignores `input_N_*` anyway. */
function typedInputGroup(n: number): Array<Record<string, unknown>> {
  const group = `typed-input-${n}`;
  return [
    {
      label: "Name",
      type: "String",
      optional: true,
      placeholder: "e.g., firstName",
      tooltip:
        "Argument name in the tool's JSON input schema. `businessId` is reserved.",
      binding: toolProperty(`input_${n}_name`),
      group,
    },
    {
      label: "Type",
      type: "Dropdown",
      optional: true,
      choices: MCP_INPUT_JSON_SCHEMA_TYPES.map((t) => ({ name: t, value: t })),
      binding: toolProperty(`input_${n}_type`),
      group,
    },
    {
      label: "Description",
      type: "Text",
      optional: true,
      binding: toolProperty(`input_${n}_description`),
      group,
    },
    {
      label: "Required",
      type: "Dropdown",
      optional: true,
      choices: [
        { name: "Yes", value: "true" },
        { name: "No", value: "false" },
      ],
      binding: toolProperty(`input_${n}_required`),
      group,
    },
  ];
}

const typedInputIndexes = Array.from(
  { length: MCP_TYPED_INPUT_GROUPS },
  (_, i) => i + 1,
);

/**
 * MCP start event — Nano's copy of Camunda's `io.camunda.mcp.start-message`
 * template (camunda/connectors `agenticai-mcp-message-start-event.json`). Same
 * id, version and `io.camunda.tool:*` bindings, so models round-trip with
 * Camunda Modeler unchanged; plus optional typed-input groups that
 * `/mcp/processes` turns into a typed `inputSchema`.
 */
export const mcpStartEvent: ElementTemplate = {
  $schema: ELEMENT_TEMPLATE_SCHEMA_URL,
  name: "MCP start event",
  id: "io.camunda.mcp.start-message",
  version: 1,
  description: "Expose the process as MCP tool",
  appliesTo: ["bpmn:StartEvent"],
  engines: { camunda: "^8.10" },
  entriesVisible: { outputs: true },
  elementType: {
    value: "bpmn:StartEvent",
    eventDefinition: "bpmn:MessageEventDefinition",
  },
  groups: [
    { id: "tool-definition", label: "Tool definition", openByDefault: true },
    ...typedInputIndexes.map((n) => ({
      id: `typed-input-${n}`,
      label: `Typed input ${n} (Nano, optional)`,
      openByDefault: false,
    })),
  ],
  category: { id: "aiTools", name: "AI Tools" },
  properties: [
    {
      label: "Name",
      tooltip:
        "Give this process a name that helps AI agents understand what it does. <br/><br/>Use only letters, numbers, hyphens (-), underscores (_), and dots (.). Spaces are not allowed.",
      type: "String",
      placeholder: "e.g., my_process",
      binding: toolProperty("name"),
      constraints: {
        notEmpty: true,
        pattern: "^[a-zA-Z-_.0-9]+$",
        maxLength: 100,
      },
      group: "tool-definition",
    },
    {
      label: "What it does",
      type: "Text",
      placeholder:
        "Describe the core function of this process in a clear and concise way.",
      binding: toolProperty("purpose"),
      constraints: { notEmpty: true },
      group: "tool-definition",
    },
    {
      label: "Which inputs it needs",
      type: "Text",
      placeholder:
        "Describe the inputs the AI should send to start this process.",
      tooltip:
        "Provide the inputs the AI should send along when this process is started, in plain language.<br/><br/>You can describe optional and required parameters, their data types and formats, and any constraints they have, such as a limited set of options the AI can choose from.",
      binding: toolProperty("inputs"),
      constraints: { notEmpty: true },
      group: "tool-definition",
    },
    {
      label: "When to use",
      type: "Text",
      placeholder:
        "Describe the specific situations or user intents that should trigger this process.",
      binding: toolProperty("when_to_use"),
      group: "tool-definition",
    },
    {
      label: "When not to use",
      type: "Text",
      placeholder:
        "Describe the situations or conditions where this process should not be used.",
      binding: toolProperty("when_not_to_use"),
      group: "tool-definition",
    },
    {
      label: "What the tool returns",
      tooltip:
        "Describe the outcomes and results after the process completes. You can also mention specific variable names that will be available (e.g., 'iban', 'request_id').",
      type: "Text",
      placeholder: "Describe the expected outcome and what data is returned.",
      binding: toolProperty("results"),
      group: "tool-definition",
    },
    {
      id: "messageNameUuid",
      type: "Hidden",
      generatedValue: { type: "uuid" },
      binding: { type: "bpmn:Message#property", name: "name" },
      group: "tool-definition",
    },
    ...typedInputIndexes.flatMap(typedInputGroup),
  ],
};

/** Components every project's modeler offers, regardless of packs/project. */
export const BUILTIN_COMPONENTS: readonly ElementTemplate[] = [mcpStartEvent];
