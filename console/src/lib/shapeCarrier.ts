// Composed motion-shape carrier (ADR 0040 §9) --------------------------------
// A composed shape travels *in the BPMN model* as a `nano:shape` declaration in a
// `nano:shapes` container on the defining `bpmn:process`'s extension elements, in
// the dedicated nano moddle namespace (see `moddle/nanoShapes.ts`). Each shape has
// an `id` (its fuse identity + `DomainTypes` key) and an ordered list of the four
// composition ops (carry / project / extend / reference); XML order is the
// author-order fold the reifier (`server/src/console/domain_types.ts::resolveShapes`)
// depends on, so read/write preserve it.
//
// These helpers are pure (the moddle/modeling services are injected), so they live
// here — apart from the browser-only bpmn-js modeler component — to keep the
// model-carrier logic unit-testable. See shapeCarrier.test.ts. The wire shapes
// (`ShapeDecl`/`ShapeOp`) mirror the server-side reifier's types so a round-trip
// through bpmn-js reproduces exactly what the Rust scan lifts.

// Minimal structural view of a moddle business object. The bpmn-js modeler's
// richer `ModdleElement` is a superset and assigns structurally.
export interface ShapeModdleElement {
  $type?: string;
  id?: string;
  name?: string;
  ref?: string;
  fields?: string;
  via?: string;
  type?: string;
  optional?: boolean;
  list?: boolean;
  spread?: boolean;
  description?: string;
  key?: string;
  value?: string;
  shapes?: ShapeModdleElement[];
  ops?: ShapeModdleElement[];
  values?: ShapeModdleElement[];
  extensionElements?: ShapeModdleElement;
  $parent?: unknown;
}

export interface ShapeModdle {
  create(type: string, attrs?: Record<string, unknown>): ShapeModdleElement;
}

export interface ShapeModeling {
  updateModdleProperties(
    element: unknown,
    moddleElement: unknown,
    props: Record<string, unknown>,
  ): void;
}

/** One composition op of a shape, in author (XML) order (mirrors the reifier). */
export type ShapeOp =
  | { op: "carry"; ref: string }
  | { op: "project"; ref: string; fields: string[]; via?: string }
  | {
      op: "extend";
      name: string;
      type: string;
      optional?: boolean;
      list?: boolean;
      description?: string;
    }
  | {
      op: "reference";
      name: string;
      ref: string;
      spread?: boolean;
      list?: boolean;
      description?: string;
    };

/** A composed motion-shape declaration carried on the process (mirrors the reifier). */
export interface ShapeDecl {
  id: string;
  name?: string;
  ops: ShapeOp[];
}

/** A flat scalar field of a shape, as edited by the "Data envelope" field editor. */
export interface EnvelopeShapeField {
  name: string;
  type: string;
  optional: boolean;
}

/**
 * The flat scalar field list of a shape, or `null` if the shape uses any op the
 * flat envelope field editor can't round-trip losslessly — composition
 * (`carry`/`project`/`reference`), a `list` field, or a field carrying a
 * `description` (the flat editor has no description column, so saving would
 * drop it — and the description feeds the MCP tool `inputSchema`, #1738). Drives both whether the
 * picker offers "Edit fields…" and the editor's pre-fill: an empty shape or one
 * with a single non-`extend` op returns `null` so those types stay in the shape
 * composer instead of silently losing ops on save.
 */
export function envelopeEditableFields(
  shape: ShapeDecl | undefined,
): EnvelopeShapeField[] | null {
  if (!shape || shape.ops.length === 0) return null;
  const fields = shape.ops.flatMap((o) =>
    o.op === "extend" && !o.list && !o.description
      ? [{ name: o.name, type: o.type, optional: !!o.optional }]
      : [],
  );
  return fields.length === shape.ops.length ? fields : null;
}

/** The moddle `$type` for a `nano:shapes` container. */
export const SHAPES_TYPE = "nano:Shapes";
/** The moddle `$type` for one `nano:shape`. */
export const SHAPE_TYPE = "nano:Shape";
/** The moddle `$type` for one model-level `nano:meta` entry (ADR 0040 §5). */
export const META_TYPE = "nano:Meta";

/** One model-level metadata entry carried as a `nano:meta` sibling of the
 * `nano:shapes` container on a process's extension elements (ADR 0040 §5). */
export interface MetaEntry {
  key: string;
  value: string;
}

const OP_TYPE: Record<ShapeOp["op"], string> = {
  carry: "nano:Carry",
  project: "nano:Project",
  extend: "nano:Extend",
  reference: "nano:Reference",
};

/** The local (namespace-stripped) name of a moddle `$type`. */
function localType(t: string | undefined): string {
  return (t ?? "").split(":").pop() ?? "";
}

/** The `nano:shapes` container on a process's extension elements, if present. */
export function shapesContainer(
  processBo: ShapeModdleElement | undefined,
): ShapeModdleElement | undefined {
  return (processBo?.extensionElements?.values ?? []).find(
    (v) => localType(v.$type) === "Shapes",
  );
}

/** Split a `nano:project fields="a, b"` attribute into trimmed, non-empty names. */
function splitFields(raw: string | undefined): string[] {
  return (raw ?? "")
    .split(",")
    .map((s) => s.trim())
    .filter((s) => s.length > 0);
}

/** Parse one op moddle element into a `ShapeOp` (or `undefined` when malformed —
 * missing the identifying `ref`, or an `extend` missing `name`/`type`). */
function readOp(el: ShapeModdleElement): ShapeOp | undefined {
  const t = localType(el.$type);
  const ref = el.ref?.trim();
  switch (t) {
    case "Carry":
      return ref ? { op: "carry", ref } : undefined;
    case "Project": {
      if (!ref) return undefined;
      // A project with no field names is a silent no-op (`carry` spreads all
      // fields), so treat an empty list as malformed and drop it.
      const fields = splitFields(el.fields);
      if (fields.length === 0) return undefined;
      const op: ShapeOp = { op: "project", ref, fields };
      const via = el.via?.trim();
      if (via) op.via = via;
      return op;
    }
    case "Extend": {
      const name = el.name?.trim();
      const type = el.type?.trim();
      if (!name || !type) return undefined;
      const op: ShapeOp = { op: "extend", name, type };
      if (el.optional) op.optional = true;
      if (el.list) op.list = true;
      const description = el.description?.trim();
      if (description) op.description = description;
      return op;
    }
    case "Reference": {
      const name = el.name?.trim();
      if (!name || !ref) return undefined;
      const op: ShapeOp = { op: "reference", name, ref };
      if (el.spread) op.spread = true;
      if (el.list) op.list = true;
      const description = el.description?.trim();
      if (description) op.description = description;
      return op;
    }
    default:
      return undefined;
  }
}

/** Read the composed shapes declared on a process (empty when none). Malformed
 * ops are dropped and a shape with no `id` is skipped, mirroring the Rust scan. */
export function readShapes(
  processBo: ShapeModdleElement | undefined,
): ShapeDecl[] {
  const container = shapesContainer(processBo);
  const out: ShapeDecl[] = [];
  for (const s of container?.shapes ?? []) {
    const id = s.id?.trim();
    if (!id) continue;
    const ops: ShapeOp[] = [];
    for (const opEl of s.ops ?? []) {
      const op = readOp(opEl);
      if (op) ops.push(op);
    }
    const decl: ShapeDecl = { id, ops };
    const name = s.name?.trim();
    if (name) decl.name = name;
    out.push(decl);
  }
  return out;
}

/** Build a moddle op element for a `ShapeOp`, attaching `$parent` for a clean
 * serialisation. Undefined/false flags are omitted so the `.bpmn` stays minimal. */
function buildOp(
  moddle: ShapeModdle,
  op: ShapeOp,
  parent: ShapeModdleElement,
): ShapeModdleElement {
  let el: ShapeModdleElement;
  switch (op.op) {
    case "carry":
      el = moddle.create(OP_TYPE.carry, { ref: op.ref });
      break;
    case "project": {
      const attrs: Record<string, unknown> = {
        ref: op.ref,
        fields: op.fields.join(", "),
      };
      if (op.via) attrs.via = op.via;
      el = moddle.create(OP_TYPE.project, attrs);
      break;
    }
    case "extend": {
      const attrs: Record<string, unknown> = { name: op.name, type: op.type };
      if (op.optional) attrs.optional = true;
      if (op.list) attrs.list = true;
      if (op.description) attrs.description = op.description;
      el = moddle.create(OP_TYPE.extend, attrs);
      break;
    }
    case "reference": {
      const attrs: Record<string, unknown> = { name: op.name, ref: op.ref };
      if (op.spread) attrs.spread = true;
      if (op.list) attrs.list = true;
      if (op.description) attrs.description = op.description;
      el = moddle.create(OP_TYPE.reference, attrs);
      break;
    }
  }
  el.$parent = parent;
  return el;
}

/** Build a `nano:Shape` moddle element from a `ShapeDecl`. */
function buildShape(
  moddle: ShapeModdle,
  decl: ShapeDecl,
  parent: ShapeModdleElement,
): ShapeModdleElement {
  const attrs: Record<string, unknown> = { id: decl.id };
  if (decl.name) attrs.name = decl.name;
  const shape = moddle.create(SHAPE_TYPE, attrs);
  shape.ops = decl.ops.map((op) => buildOp(moddle, op, shape));
  shape.$parent = parent;
  return shape;
}

/** Build a `nano:Shapes` container moddle element from a list of shapes. */
export function buildShapesContainer(
  moddle: ShapeModdle,
  shapes: ShapeDecl[],
): ShapeModdleElement {
  const container = moddle.create(SHAPES_TYPE, {});
  container.shapes = shapes.map((s) => buildShape(moddle, s, container));
  return container;
}

/**
 * Replace the process's composed shapes with `shapes`, as a single undoable
 * command. Rebuilds the `nano:shapes` container (creating `bpmn:extensionElements`
 * on demand). An empty `shapes` list removes the container so an emptied process
 * carries no stray element. Model-level `nano:meta` siblings (ADR 0040 §5) are
 * preserved — only the `nano:Shapes` container is swapped.
 */
export function writeShapes(
  moddle: ShapeModdle,
  modeling: ShapeModeling,
  element: unknown,
  processBo: ShapeModdleElement,
  shapes: ShapeDecl[],
): void {
  const ext = processBo.extensionElements;
  const existing = ext?.values ?? [];
  const container =
    shapes.length > 0 ? buildShapesContainer(moddle, shapes) : undefined;
  // Replace the `nano:Shapes` container *in place* (preserving the order of sibling
  // extension elements like `nano:meta`); only append when none existed.
  const idx = existing.findIndex((v) => localType(v.$type) === "Shapes");
  let next: ShapeModdleElement[];
  if (idx >= 0) {
    next = existing.slice();
    if (container) next[idx] = container;
    else next.splice(idx, 1);
  } else {
    next = container ? [...existing, container] : existing.slice();
  }
  if (ext) {
    for (const v of next) v.$parent = ext;
    modeling.updateModdleProperties(element, ext, { values: next });
    return;
  }
  if (next.length === 0) return;
  const newExt = moddle.create("bpmn:ExtensionElements", { values: next });
  for (const v of next) v.$parent = newExt;
  newExt.$parent = processBo;
  modeling.updateModdleProperties(element, processBo, {
    extensionElements: newExt,
  });
}

/** Read the model-level `nano:meta` entries carried on a process (empty when
 * none). Entries missing a `key` are skipped; the last write wins per key on the
 * server, so duplicates are preserved here in author order. */
export function readMeta(
  processBo: ShapeModdleElement | undefined,
): MetaEntry[] {
  const values = processBo?.extensionElements?.values ?? [];
  const out: MetaEntry[] = [];
  for (const v of values) {
    if (localType(v.$type) !== "Meta") continue;
    const key = v.key?.trim();
    if (!key) continue;
    out.push({ key, value: (v.value ?? "").trim() });
  }
  return out;
}

/** Build a `nano:Meta` moddle element from a `MetaEntry`. */
function buildMeta(
  moddle: ShapeModdle,
  entry: MetaEntry,
  parent: ShapeModdleElement,
): ShapeModdleElement {
  const el = moddle.create(META_TYPE, { key: entry.key, value: entry.value });
  el.$parent = parent;
  return el;
}

/**
 * Replace the process's model-level metadata with `meta`, as a single undoable
 * command. Swaps the `nano:meta` siblings *in place* (preserving the position and
 * the `nano:Shapes` container). Entries with a blank key are dropped. An empty
 * `meta` list removes every `nano:meta`, and if that empties the extension
 * elements entirely they are left as an empty container (bpmn-js prunes on save).
 */
export function writeMeta(
  moddle: ShapeModdle,
  modeling: ShapeModeling,
  element: unknown,
  processBo: ShapeModdleElement,
  meta: MetaEntry[],
): void {
  const clean = meta
    .filter((m) => m.key.trim().length > 0)
    .map((m) => ({ key: m.key.trim(), value: m.value.trim() }));
  const ext = processBo.extensionElements;
  const existing = ext?.values ?? [];
  const nonMeta = existing.filter((v) => localType(v.$type) !== "Meta");
  // Preserve position: splice the fresh `nano:meta` run in where the first one was
  // (or append after the existing siblings when the process had none).
  const at = existing.findIndex((v) => localType(v.$type) === "Meta");
  const built = clean.map((m) => buildMeta(moddle, m, ext ?? processBo));
  let next: ShapeModdleElement[];
  if (at >= 0) {
    // Rebuild by keeping non-meta order and inserting the meta run at `at`, counted
    // against the non-meta list so the container's relative order is stable.
    const before = existing
      .slice(0, at)
      .filter((v) => localType(v.$type) !== "Meta");
    const after = existing
      .slice(at)
      .filter((v) => localType(v.$type) !== "Meta");
    next = [...before, ...built, ...after];
  } else {
    next = [...nonMeta, ...built];
  }
  if (ext) {
    for (const v of next) v.$parent = ext;
    modeling.updateModdleProperties(element, ext, { values: next });
    return;
  }
  if (next.length === 0) return;
  const newExt = moddle.create("bpmn:ExtensionElements", { values: next });
  for (const v of next) v.$parent = newExt;
  newExt.$parent = processBo;
  modeling.updateModdleProperties(element, processBo, {
    extensionElements: newExt,
  });
}
