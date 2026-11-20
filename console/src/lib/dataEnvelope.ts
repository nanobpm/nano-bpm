// Data envelope (ADR 0033 §6) -----------------------------------------------
// A task's typed data boundary travels *in the BPMN model* as two reserved
// `zeebe:property` keys carrying a manifest `types` id — `in` (data the element
// receives) and `out` (data it produces). The envelope lives on the element for
// tasks, and on the shared `bpmn:Message` (reached through `messageRef`) for
// message-bearing elements — the `bpmn:message` + `messageRef` split BPMN uses
// for the message name, applied to its payload type. A **none start event** (a
// `bpmn:StartEvent` with no event definition — how processes started over the
// API or from a call activity begin) carries its envelope on the start event
// itself, input only: it produces the process's initial variables, so there is
// no `.out`. Absent = untyped.
//
// These helpers are pure (the moddle/modeling services are injected), so they
// live here — apart from the browser-only bpmn-js modeler component — to keep
// the model-carrier logic unit-testable. See dataEnvelope.test.ts.

// Minimal structural view of a moddle business object. The bpmn-js modeler's
// richer `ModdleElement` is a superset and assigns structurally.
export interface EnvModdleElement {
  $type?: string;
  type?: string;
  id?: string;
  name?: string;
  value?: string;
  formId?: string;
  values?: EnvModdleElement[];
  properties?: EnvModdleElement[];
  eventDefinitions?: EnvModdleElement[];
  messageRef?: EnvModdleElement;
  extensionElements?: EnvModdleElement;
  $parent?: unknown;
}

export interface EnvModdle {
  create(type: string, attrs?: Record<string, unknown>): EnvModdleElement;
}

export interface EnvModeling {
  updateModdleProperties(
    element: unknown,
    moddleElement: unknown,
    props: Record<string, unknown>,
  ): void;
}

export type EnvelopeField = "inputType" | "outputType";

// The service-ish task types whose worker keys a manifest `workers[]` entry.
export const SERVICE_TASK_TYPES = new Set([
  "bpmn:ServiceTask",
  "bpmn:BusinessRuleTask",
  "bpmn:ScriptTask",
  "bpmn:SendTask",
]);

export const ENVELOPE_KEY: Record<EnvelopeField, string> = {
  inputType: "io.nanobpm.dataEnvelope.in",
  outputType: "io.nanobpm.dataEnvelope.out",
};

// Sentinel option value: picking it opens the create-a-new-type flow.
export const CREATE_ENVELOPE = "\u0000__create_envelope__";

// Sentinel option value: picking it opens the edit-fields flow for the
// currently-selected type (only offered for editable model shapes).
export const EDIT_ENVELOPE = "\u0000__edit_envelope__";

export interface EnvelopeContext {
  // The moddle object the reserved properties are read from / written to.
  target: EnvModdleElement;
  // For service-ish tasks: the worker task type, so the envelope can be
  // projected onto the manifest `workers[]` entry that keeps `defineWorker` typed.
  taskType?: string;
  // For user tasks: the linked form id (`zeebe:formDefinition:formId`), if any,
  // so the envelope can default to the form's bound type (ADR 0033 §6 / 0029 §5).
  formId?: string;
  // True when the carrier is input-only — a none start event produces the
  // process's initial variables, so it has an `in` envelope but no `out`.
  inputOnly?: boolean;
}

// The `bpmn:Message` a message-bearing element references, if any.
export function referencedMessage(
  bo: EnvModdleElement | undefined,
): EnvModdleElement | undefined {
  if (!bo) return undefined;
  if (bo.$type === "bpmn:ReceiveTask") return bo.messageRef;
  const evd = (bo.eventDefinitions ?? []).find(
    (d) => d.$type === "bpmn:MessageEventDefinition",
  );
  return evd?.messageRef;
}

// The linked form id a user task references via `zeebe:FormDefinition:formId`
// (a Camunda "linked form"), if any. An inline `formKey`/embedded form has no id.
export function userTaskFormId(
  bo: EnvModdleElement | undefined,
): string | undefined {
  const fd = (bo?.extensionElements?.values ?? []).find(
    (v) => v.$type === "zeebe:FormDefinition",
  );
  const id = fd?.formId;
  return typeof id === "string" && id ? id : undefined;
}

// A **none** start event: a `bpmn:StartEvent` carrying no event definition (no
// message/timer/signal/… trigger). This is how a process started over the API or
// from a call activity begins, so it is where the process's start inputs are
// declared. A message start is *not* none — its envelope stays on the referenced
// `bpmn:Message` (handled by `referencedMessage` above).
export function isNoneStartEvent(bo: EnvModdleElement | undefined): boolean {
  return (
    bo?.$type === "bpmn:StartEvent" && (bo.eventDefinitions ?? []).length === 0
  );
}

// Whether `element` supports a data envelope, and where it is carried. Returns
// undefined for elements with no typed data boundary (or a message with no
// message assigned yet, and a service task with no literal worker type).
export function envelopeContext(
  element: { type?: string; businessObject?: EnvModdleElement } | undefined,
): EnvelopeContext | undefined {
  const bo = element?.businessObject;
  if (!element?.type || !bo) return undefined;
  const msg = referencedMessage(bo);
  if (msg) return { target: msg };
  if (SERVICE_TASK_TYPES.has(element.type)) {
    const td = (bo.extensionElements?.values ?? []).find(
      (v) => v.$type === "zeebe:TaskDefinition",
    );
    const t = td?.type;
    // Only a literal (non-FEEL) task type keys a worker; skip `=expr` types.
    if (typeof t !== "string" || !t || t.startsWith("=")) return undefined;
    return { target: bo, taskType: t };
  }
  if (element.type === "bpmn:UserTask")
    return { target: bo, formId: userTaskFormId(bo) };
  if (element.type === "bpmn:StartEvent" && isNoneStartEvent(bo))
    return { target: bo, inputOnly: true };
  return undefined;
}

// Every domain-type id referenced by an envelope (input or output) across a set
// of elements. The model is the authoritative carrier of the data contract (the
// reserved `zeebe:property`), so a type can be *referenced* by an envelope without
// being declared in the manifest `types` registry — e.g. a hand-authored or
// drifted model. Deriving the referenced ids lets the picker surface (and keep
// selectable) those types rather than rendering a set envelope as blank because
// its id is absent from the authored registry. Deduped; order is insertion order.
export function collectEnvelopeTypeRefs(
  elements: (
    { type?: string; businessObject?: EnvModdleElement } | undefined
  )[],
): string[] {
  const ids = new Set<string>();
  for (const el of elements) {
    const ctx = envelopeContext(el);
    if (!ctx) continue;
    for (const field of ["inputType", "outputType"] as EnvelopeField[]) {
      const v = readEnvelope(ctx.target, field);
      if (v) ids.add(v);
    }
  }
  return [...ids];
}

export function zeebePropsContainer(
  bo: EnvModdleElement | undefined,
): EnvModdleElement | undefined {
  return (bo?.extensionElements?.values ?? []).find(
    (v) => v.$type === "zeebe:Properties",
  );
}

// Read the envelope ref currently on `target` (or "" when unset).
export function readEnvelope(
  target: EnvModdleElement | undefined,
  field: EnvelopeField,
): string {
  const p = (zeebePropsContainer(target)?.properties ?? []).find(
    (x) => x.name === ENVELOPE_KEY[field],
  );
  return p && typeof p.value === "string" ? p.value : "";
}

// Write (or clear, on "") the envelope ref on `target`, as a single undoable
// command. Creates the `zeebe:Properties` container (and `bpmn:extensionElements`)
// on demand so the edit is one command in every case.
export function writeEnvelope(
  moddle: EnvModdle,
  modeling: EnvModeling,
  element: unknown,
  target: EnvModdleElement,
  field: EnvelopeField,
  value: string,
): void {
  const name = ENVELOPE_KEY[field];
  const container = zeebePropsContainer(target);
  const kept = (container?.properties ?? []).filter((p) => p.name !== name);
  const props = value
    ? [...kept, moddle.create("zeebe:Property", { name, value })]
    : kept;
  if (container) {
    for (const p of props) p.$parent = container;
    modeling.updateModdleProperties(element, container, { properties: props });
    return;
  }
  const newContainer = moddle.create("zeebe:Properties", { properties: props });
  for (const p of props) p.$parent = newContainer;
  const ext = target.extensionElements;
  if (ext) {
    newContainer.$parent = ext;
    modeling.updateModdleProperties(element, ext, {
      values: [...(ext.values ?? []), newContainer],
    });
  } else {
    const newExt = moddle.create("bpmn:ExtensionElements", {
      values: [newContainer],
    });
    newContainer.$parent = newExt;
    newExt.$parent = target;
    modeling.updateModdleProperties(element, target, {
      extensionElements: newExt,
    });
  }
}
