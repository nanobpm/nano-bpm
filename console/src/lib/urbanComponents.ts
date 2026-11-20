// Urban components — element templates that turn a generic BPMN task into a
// typed, configured component with its own properties panel and palette entry
// (ADR 0033). This is the Delphi-component analog for the process canvas: the
// element template is a component's *design-time face*; its `taskDefinition`
// type is the seam to a runtime `workers[].taskType` (ADR 0022), and its
// input/output properties are FEEL fields that will be scoped to the bound
// domain type (ADR 0029, generalising the DMN injection shipped in #197).
//
// SAMPLE SET: these reference components document the element-template shape and
// seed a project's `components/` dir for the demo. The modeler no longer imports
// this constant — it loads the installed set from the open project (ADR 0033
// increment 2, `projectComponents.ts`); packs (ADR 0007) layer on in increment 6.

import { ELEMENT_TEMPLATE_SCHEMA_URL } from "./elementTemplateSchema";

/** The Zeebe (Camunda 8 / "Cloud") element-template JSON shape. Typed loosely —
 *  the authoritative schema is `@camunda/zeebe-element-templates-json-schema`,
 *  which the modeler's validator enforces at `elementTemplates.set()`. */
export interface ElementTemplate {
  $schema?: string;
  id: string;
  name: string;
  description?: string;
  version?: number;
  appliesTo: string[];
  engines?: Record<string, string>;
  entriesVisible?: Record<string, boolean>;
  elementType?: { value: string; eventDefinition?: string };
  groups?: Array<{ id: string; label: string; openByDefault?: boolean }>;
  category?: { id: string; name: string };
  properties: Array<Record<string, unknown>>;
}

// The `$schema` the seed templates carry. Imported from the schema-registration
// module so the URL written into these files can never drift from the version
// whose content Monaco bundles for offline resolution (`elementTemplateSchema.ts`).
const SCHEMA = ELEMENT_TEMPLATE_SCHEMA_URL;

/**
 * Read Thermostat — mirrors ADR 0022's `read-thermostat` worker. A service task
 * pre-bound to that task type, with one FEEL input (`room`) and one output
 * variable.
 */
const readThermostat: ElementTemplate = {
  $schema: SCHEMA,
  id: "io.nanobpm.urban.read-thermostat",
  name: "Read Thermostat",
  description: "Read a room's current temperature (Urban component).",
  appliesTo: ["bpmn:Task"],
  elementType: { value: "bpmn:ServiceTask" },
  properties: [
    {
      type: "Hidden",
      value: "read-thermostat",
      binding: { type: "zeebe:taskDefinition:type" },
    },
    {
      label: "Room",
      type: "String",
      feel: "optional",
      binding: { type: "zeebe:input", name: "room" },
    },
    {
      label: "Result variable",
      type: "String",
      value: "temperature",
      binding: { type: "zeebe:output", source: "= temperature" },
    },
  ],
};

/**
 * Classify (LLM) — mirrors ADR 0022's (E) LLM-as-worker. A service task bound to
 * the `classify` task type with a FEEL `text` input and a categorised output.
 */
const classifyLlm: ElementTemplate = {
  $schema: SCHEMA,
  id: "io.nanobpm.urban.classify-llm",
  name: "Classify (LLM)",
  description: "Classify text with an LLM job worker (Urban component).",
  appliesTo: ["bpmn:Task"],
  elementType: { value: "bpmn:ServiceTask" },
  properties: [
    {
      type: "Hidden",
      value: "classify",
      binding: { type: "zeebe:taskDefinition:type" },
    },
    {
      label: "Text",
      type: "String",
      feel: "required",
      binding: { type: "zeebe:input", name: "text" },
    },
    {
      label: "Categories (comma separated)",
      type: "String",
      binding: { type: "zeebe:input", name: "categories" },
    },
    {
      label: "Result variable",
      type: "String",
      value: "category",
      binding: { type: "zeebe:output", source: "= category" },
    },
  ],
};

/** The bundled sample components surfaced in the palette and template chooser. */
export const urbanComponents: ElementTemplate[] = [readThermostat, classifyLlm];
