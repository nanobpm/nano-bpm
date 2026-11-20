// Pure editing + preview helpers for the composed motion-shape composer (ADR 0040
// §9/§10). The React surface (`ShapeComposer.tsx`) is a thin shell over these; the
// model round-trip stays in `shapeCarrier.ts` (undoable moddle commands) and the
// resolution stays server-side (the `domaintypes` preview endpoint) — this module
// only manipulates the in-memory `ShapeDecl[]` and parses the emitted preview text,
// so every rule here is unit-testable without a modeler or a server.

import type { ShapeDecl, ShapeOp } from "./shapeCarrier";

/** The four composition operations (ADR 0040 §9). */
export type OpKind = ShapeOp["op"];

export const OP_KINDS: OpKind[] = ["carry", "project", "extend", "reference"];

/** Primitive field keywords the emitter understands for an `extend` op's `type`
 * (ADR 0029 §4.2); anything else is a nominal reference to another registry type
 * (a manifest type or a composed shape — never a table, ADR 0040 §10). */
export const SCALAR_KEYWORDS = [
  "string",
  "number",
  "integer",
  "boolean",
  "date",
  "datetime",
  "json",
] as const;

/** A leaf entity the composer's pickers can reference: a DB table, a manifest
 * `type`, or another composed shape. `fields` are the names available to a
 * `project`/`extend`-from; `fks` (DB tables only) drive the `project via` picker. */
export interface ComposerEntity {
  id: string;
  kind: "table" | "type" | "shape";
  fields: string[];
  /** FK column → referenced entity id, for `project via` path validation. */
  fks?: { column: string; refId: string }[];
}

/** Which op kinds carry a `ref` (a source entity) vs a `name` (a new field). */
const OP_HAS_REF: Record<OpKind, boolean> = {
  carry: true,
  project: true,
  extend: false,
  reference: true,
};
const OP_HAS_NAME: Record<OpKind, boolean> = {
  carry: false,
  project: false,
  extend: true,
  reference: true,
};

/** A fresh op of `kind` with its required fields defaulted (empty). */
export function newOp(kind: OpKind): ShapeOp {
  switch (kind) {
    case "carry":
      return { op: "carry", ref: "" };
    case "project":
      return { op: "project", ref: "", fields: [] };
    case "extend":
      return { op: "extend", name: "", type: "string" };
    case "reference":
      return { op: "reference", name: "", ref: "" };
  }
}

/** Retype an op, carrying `ref`/`name` across kinds that share them so a maker
 * can flip carry↔project↔reference (or extend↔reference) without retyping. */
export function changeOpKind(op: ShapeOp, kind: OpKind): ShapeOp {
  if (op.op === kind) return op;
  const next = newOp(kind);
  const ref = "ref" in op ? op.ref : "";
  const name = "name" in op ? op.name : "";
  if (OP_HAS_REF[kind] && ref && "ref" in next) next.ref = ref;
  if (OP_HAS_NAME[kind] && name && "name" in next) next.name = name;
  // extend <-> reference both carry a field description (MCP docs, #1738).
  const description = "description" in op ? op.description : undefined;
  if (description && (next.op === "extend" || next.op === "reference")) {
    next.description = description;
  }
  return next;
}

/** Shallow-merge `patch` onto op `i` (call sites keep the union consistent). */
export function updateOp(
  ops: ShapeOp[],
  i: number,
  patch: Record<string, unknown>,
): ShapeOp[] {
  if (i < 0 || i >= ops.length) return ops;
  const next = ops.slice();
  next[i] = { ...ops[i], ...patch } as ShapeOp;
  return next;
}

export function addOp(ops: ShapeOp[], op: ShapeOp): ShapeOp[] {
  return [...ops, op];
}

/** Replace op `i` outright. Used when retyping: `changeOpKind` returns a complete
 * fresh op, so a shallow merge (`updateOp`) would leave stale kind-incompatible
 * fields behind (e.g. `fields`/`via` lingering on a `carry`), which then leak into
 * the preview request body. */
export function replaceOp(ops: ShapeOp[], i: number, op: ShapeOp): ShapeOp[] {
  if (i < 0 || i >= ops.length) return ops;
  const next = ops.slice();
  next[i] = op;
  return next;
}

export function removeOp(ops: ShapeOp[], i: number): ShapeOp[] {
  return ops.filter((_, k) => k !== i);
}

/** Move op `i` by `dir` (−1 up / +1 down), preserving author order elsewhere.
 * Author order is the fold order the reifier depends on, so reordering is a
 * first-class edit (ADR 0040 §9). A no-op at the ends. */
export function moveOp(ops: ShapeOp[], i: number, dir: -1 | 1): ShapeOp[] {
  const j = i + dir;
  if (i < 0 || i >= ops.length || j < 0 || j >= ops.length) return ops;
  const next = ops.slice();
  [next[i], next[j]] = [next[j], next[i]];
  return next;
}

/** Toggle a projected field name on `project` op `i` (no-op on other kinds). */
export function toggleProjectField(
  ops: ShapeOp[],
  i: number,
  field: string,
): ShapeOp[] {
  const op = ops[i];
  if (!op || op.op !== "project") return ops;
  const has = op.fields.includes(field);
  const fields = has
    ? op.fields.filter((f) => f !== field)
    : [...op.fields, field];
  return updateOp(ops, i, { fields });
}

export function addShape(shapes: ShapeDecl[], id: string): ShapeDecl[] {
  return [...shapes, { id, ops: [] }];
}

export function removeShape(shapes: ShapeDecl[], i: number): ShapeDecl[] {
  return shapes.filter((_, k) => k !== i);
}

export function updateShape(
  shapes: ShapeDecl[],
  i: number,
  patch: Partial<ShapeDecl>,
): ShapeDecl[] {
  if (i < 0 || i >= shapes.length) return shapes;
  const next = shapes.slice();
  next[i] = { ...shapes[i], ...patch };
  return next;
}

/** A collision-free shape id derived from `base` given the ids already in use. */
export function uniqueShapeId(
  existing: Iterable<string>,
  base = "Shape",
): string {
  const used = new Set(existing);
  if (!used.has(base)) return base;
  for (let n = 2; ; n++) {
    const candidate = `${base}${n}`;
    if (!used.has(candidate)) return candidate;
  }
}

export function entityById(
  entities: ComposerEntity[],
  id: string,
): ComposerEntity | undefined {
  return entities.find((e) => e.id === id);
}

/** The field names an entity exposes, or `[]` when it is unknown. */
export function fieldsOf(entities: ComposerEntity[], id: string): string[] {
  return entityById(entities, id)?.fields ?? [];
}

/** Build `ComposerEntity`s for the shapes themselves, so a `project`/`carry`/
 * `reference` can target another composed shape and see its (preview-resolved)
 * fields. `text` is the preview's emitted domain block; a shape absent from it
 * (unresolved) still appears, with no fields. */
export function shapeEntities(
  shapes: ShapeDecl[],
  text: string | undefined,
): ComposerEntity[] {
  return shapes
    .filter((s) => s.id)
    .map((s) => ({
      id: s.id,
      kind: "shape" as const,
      fields:
        (text ? extractShapeFields(text, s.id) : null)?.map((f) => f.name) ??
        [],
    }));
}

/** A resolved field parsed from the preview's emitted `DomainTypes` block. */
export interface ResolvedField {
  name: string;
  type: string;
  optional: boolean;
}

/** Extract the resolved fields of `shapeId` from the emitted domain text (the
 * `"<id>": { … }` entry in the `DomainTypes` block, ADR 0029 §4.2). Returns null
 * when the shape is absent (e.g. omitted for an `error` diagnostic), so the UI can
 * distinguish "no fields" from "did not resolve". Best-effort brace matching over
 * the generator's stable emit — the preview is authoring-time only. */
export function extractShapeFields(
  text: string,
  shapeId: string,
): ResolvedField[] | null {
  const marker = `${JSON.stringify(shapeId)}:`;
  const at = text.indexOf(marker);
  if (at < 0) return null;
  const open = text.indexOf("{", at + marker.length);
  if (open < 0) return null;
  // Match the object body's braces so nested (unlikely) braces don't end it early.
  let depth = 0;
  let end = -1;
  for (let k = open; k < text.length; k++) {
    const ch = text[k];
    if (ch === "{") depth++;
    else if (ch === "}") {
      depth--;
      if (depth === 0) {
        end = k;
        break;
      }
    }
  }
  if (end < 0) return null;
  const body = text.slice(open + 1, end);
  const fields: ResolvedField[] = [];
  // Each field is `  name?: tsType;` — quoted keys keep their inner text.
  const re = /(?:"([^"]+)"|([A-Za-z_$][\w$]*))(\?)?\s*:\s*([^;]+);/g;
  let m: RegExpExecArray | null;
  while ((m = re.exec(body))) {
    fields.push({
      name: m[1] ?? m[2] ?? "",
      optional: m[3] === "?",
      type: m[4].trim(),
    });
  }
  return fields;
}
