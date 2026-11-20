import assert from "node:assert/strict";
import { test } from "node:test";

import type { ShapeOp } from "./shapeCarrier.ts";
import {
  addOp,
  addShape,
  changeOpKind,
  extractShapeFields,
  fieldsOf,
  moveOp,
  newOp,
  removeOp,
  removeShape,
  replaceOp,
  shapeEntities,
  toggleProjectField,
  uniqueShapeId,
  updateOp,
  updateShape,
  type ComposerEntity,
} from "./shapeComposer.ts";

const entities: ComposerEntity[] = [
  { id: "Order", kind: "type", fields: ["item", "qty"] },
  {
    id: "app.customers",
    kind: "table",
    fields: ["id", "name"],
    fks: [{ column: "orderId", refId: "app.orders" }],
  },
  { id: "ApprovedOrder", kind: "shape", fields: ["item", "qty", "approved"] },
];

test("newOp defaults each kind's required fields", () => {
  assert.deepEqual(newOp("carry"), { op: "carry", ref: "" });
  assert.deepEqual(newOp("project"), { op: "project", ref: "", fields: [] });
  assert.deepEqual(newOp("extend"), { op: "extend", name: "", type: "string" });
  assert.deepEqual(newOp("reference"), { op: "reference", name: "", ref: "" });
});

test("changeOpKind carries ref across ref-bearing kinds and name across name-bearing kinds", () => {
  const carry: ShapeOp = { op: "carry", ref: "Order" };
  // carry → project keeps the ref, adds an empty field set.
  assert.deepEqual(changeOpKind(carry, "project"), {
    op: "project",
    ref: "Order",
    fields: [],
  });
  // carry → extend drops the ref (extend has no ref) and defaults.
  assert.deepEqual(changeOpKind(carry, "extend"), {
    op: "extend",
    name: "",
    type: "string",
  });
  const ext: ShapeOp = { op: "extend", name: "approved", type: "boolean" };
  // extend → reference keeps the name, adds an empty ref.
  assert.deepEqual(changeOpKind(ext, "reference"), {
    op: "reference",
    name: "approved",
    ref: "",
  });
  // same kind is identity.
  assert.equal(changeOpKind(carry, "carry"), carry);
});

test("updateOp shallow-merges a patch immutably", () => {
  const ops: ShapeOp[] = [{ op: "carry", ref: "" }];
  const next = updateOp(ops, 0, { ref: "Order" });
  assert.deepEqual(next[0], { op: "carry", ref: "Order" });
  assert.deepEqual(ops[0], { op: "carry", ref: "" }); // original untouched
});

test("moveOp reorders and clamps at the ends", () => {
  const ops: ShapeOp[] = [
    { op: "carry", ref: "a" },
    { op: "carry", ref: "b" },
    { op: "carry", ref: "c" },
  ];
  assert.deepEqual(
    moveOp(ops, 2, -1).map((o) => "ref" in o && o.ref),
    ["a", "c", "b"],
  );
  assert.equal(moveOp(ops, 0, -1), ops); // top can't go up
  assert.equal(moveOp(ops, 2, 1), ops); // bottom can't go down
});

test("addOp / removeOp are immutable", () => {
  const ops: ShapeOp[] = [{ op: "carry", ref: "a" }];
  const added = addOp(ops, newOp("extend"));
  assert.equal(added.length, 2);
  assert.equal(ops.length, 1);
  assert.deepEqual(removeOp(added, 0), [added[1]]);
});

test("toggleProjectField adds then removes a field, only on project ops", () => {
  const ops: ShapeOp[] = [{ op: "project", ref: "app.customers", fields: [] }];
  const on = toggleProjectField(ops, 0, "name");
  assert.deepEqual((on[0] as { fields: string[] }).fields, ["name"]);
  const off = toggleProjectField(on, 0, "name");
  assert.deepEqual((off[0] as { fields: string[] }).fields, []);
  // A carry op is left untouched.
  const carry: ShapeOp[] = [{ op: "carry", ref: "Order" }];
  assert.equal(toggleProjectField(carry, 0, "x"), carry);
});

test("shape list ops and uniqueShapeId", () => {
  let shapes = addShape([], "Draft");
  shapes = updateShape(shapes, 0, { name: "Draft order" });
  assert.deepEqual(shapes[0], { id: "Draft", ops: [], name: "Draft order" });
  assert.equal(uniqueShapeId(["Shape"]), "Shape2");
  assert.equal(uniqueShapeId(["Shape", "Shape2"]), "Shape3");
  assert.equal(uniqueShapeId([]), "Shape");
  assert.deepEqual(removeShape(shapes, 0), []);
});

test("fieldsOf returns an entity's fields or empty", () => {
  assert.deepEqual(fieldsOf(entities, "app.customers"), ["id", "name"]);
  assert.deepEqual(fieldsOf(entities, "missing"), []);
});

test("extractShapeFields parses the emitted DomainTypes block", () => {
  const text = [
    "export interface DomainTables {}",
    "export interface DomainTypes {",
    '  "ApprovedOrder": {',
    "    item: string;",
    "    qty: number;",
    "    note?: string;",
    "    tags: string[];",
    "  };",
    '  "Empty": {};',
    "}",
  ].join("\n");
  const fields = extractShapeFields(text, "ApprovedOrder");
  assert.deepEqual(fields, [
    { name: "item", type: "string", optional: false },
    { name: "qty", type: "number", optional: false },
    { name: "note", type: "string", optional: true },
    { name: "tags", type: "string[]", optional: false },
  ]);
  assert.deepEqual(extractShapeFields(text, "Empty"), []);
  assert.equal(extractShapeFields(text, "Absent"), null);
});

test("shapeEntities exposes each shape's preview-resolved fields", () => {
  const shapes = [
    { id: "ApprovedOrder", ops: [] },
    { id: "Pending", ops: [] },
    { id: "", ops: [] },
  ];
  const text = [
    "export interface DomainTypes {",
    '  "ApprovedOrder": {',
    "    item: string;",
    "    qty?: number;",
    "  };",
    "}",
  ].join("\n");
  assert.deepEqual(shapeEntities(shapes, text), [
    { id: "ApprovedOrder", kind: "shape", fields: ["item", "qty"] },
    { id: "Pending", kind: "shape", fields: [] },
  ]);
  // Without preview text every shape appears with no fields (and blank ids drop).
  assert.deepEqual(shapeEntities(shapes, undefined), [
    { id: "ApprovedOrder", kind: "shape", fields: [] },
    { id: "Pending", kind: "shape", fields: [] },
  ]);
});

test("replaceOp swaps an op outright, leaving no stale fields (retype)", () => {
  const ops: ShapeOp[] = [
    { op: "project", ref: "Order", fields: ["a"], via: "fk" },
  ];
  // Retyping project -> carry must not leave `fields`/`via` behind.
  const next = replaceOp(ops, 0, changeOpKind(ops[0], "carry"));
  assert.deepEqual(next, [{ op: "carry", ref: "Order" }]);
  assert.notEqual(next, ops);
  // Out-of-range is a no-op returning the same array.
  assert.equal(replaceOp(ops, 5, newOp("carry")), ops);
});

test("changeOpKind preserves a field description across extend <-> reference", () => {
  const ext: ShapeOp = { op: "extend", name: "a", type: "string", description: "Doc." };
  const ref = changeOpKind(ext, "reference");
  assert.deepEqual(ref, { op: "reference", name: "a", ref: "", description: "Doc." });
  assert.deepEqual(changeOpKind(ref, "extend"), {
    op: "extend",
    name: "a",
    type: "string",
    description: "Doc.",
  });
  // carry has no description slot.
  assert.deepEqual(changeOpKind(ext, "carry"), { op: "carry", ref: "" });
});
