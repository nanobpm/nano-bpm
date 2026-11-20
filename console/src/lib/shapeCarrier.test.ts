// Unit tests for the composed motion-shape model carrier (ADR 0040 §9).
// Node-native: run with `node --experimental-strip-types --test src/lib/shapeCarrier.test.ts`.
// Covers the pure carrier logic (moddle/modeling injected) without bpmn-js, and
// the round-trip: build → read reproduces exactly what the Rust scan lifts.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  buildShapesContainer,
  envelopeEditableFields,
  readMeta,
  readShapes,
  type MetaEntry,
  type ShapeDecl,
  type ShapeModdle,
  type ShapeModdleElement,
  type ShapeModeling,
  writeMeta,
  writeShapes,
} from "./shapeCarrier.ts";

// A minimal moddle stand-in: create() returns a tagged plain object.
const moddle: ShapeModdle = {
  create(type, attrs) {
    return {
      $type: type,
      ...(attrs as Record<string, unknown>),
    } as ShapeModdleElement;
  },
};

function recordingModeling(): ShapeModeling & {
  calls: Array<{ moddleElement: unknown; props: Record<string, unknown> }>;
} {
  const calls: Array<{
    moddleElement: unknown;
    props: Record<string, unknown>;
  }> = [];
  return {
    calls,
    updateModdleProperties(_element, moddleElement, props) {
      calls.push({ moddleElement, props });
    },
  };
}

/** A process businessObject carrying a `nano:shapes` container built from decls. */
function processWithShapes(shapes: ShapeDecl[]): ShapeModdleElement {
  return {
    $type: "bpmn:Process",
    id: "orders",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [buildShapesContainer(moddle, shapes)],
    },
  };
}

const sample: ShapeDecl[] = [
  {
    id: "ApprovedOrder",
    name: "Approved order",
    ops: [
      { op: "carry", ref: "Order" },
      {
        op: "project",
        ref: "Customer",
        fields: ["tier", "region"],
        via: "Order.customerId",
      },
      { op: "extend", name: "approved", type: "boolean" },
      {
        op: "extend",
        name: "reviewedBy",
        type: "string",
        optional: true,
        description: "Who approved the order.",
      },
      {
        op: "reference",
        name: "lines",
        ref: "OrderLine",
        list: true,
        description: "The order lines.",
      },
    ],
  },
];

test("readShapes: round-trips the four-op algebra in author order", () => {
  const bo = processWithShapes(sample);
  assert.deepEqual(readShapes(bo), sample);
});

test("readShapes: returns [] when the process declares no shapes", () => {
  assert.deepEqual(readShapes({ $type: "bpmn:Process" }), []);
  assert.deepEqual(readShapes(undefined), []);
});

test("envelopeEditableFields: maps a pure-extend shape to its scalar fields", () => {
  const shape: ShapeDecl = {
    id: "orderPlaced",
    ops: [
      { op: "extend", name: "orderId", type: "string" },
      { op: "extend", name: "total", type: "number", optional: true },
    ],
  };
  assert.deepEqual(envelopeEditableFields(shape), [
    { name: "orderId", type: "string", optional: false },
    { name: "total", type: "number", optional: true },
  ]);
});

test("envelopeEditableFields: null for empty, undefined, or non-extend shapes", () => {
  assert.equal(envelopeEditableFields(undefined), null);
  assert.equal(envelopeEditableFields({ id: "empty", ops: [] }), null);
  // A composition op (carry) can't be represented by the flat field editor.
  assert.equal(
    envelopeEditableFields({
      id: "composed",
      ops: [
        { op: "extend", name: "note", type: "string" },
        { op: "carry", ref: "base" },
      ],
    }),
    null,
  );
  // A described field would lose its description on save (#1738).
  assert.equal(
    envelopeEditableFields({
      id: "described",
      ops: [{ op: "extend", name: "a", type: "string", description: "A." }],
    }),
    null,
  );
  // A `list` extend field would be dropped on save, so it's not editable here.
  assert.equal(
    envelopeEditableFields({
      id: "listy",
      ops: [{ op: "extend", name: "tags", type: "string", list: true }],
    }),
    null,
  );
});

test("readShapes: drops a shape with no id and malformed ops", () => {
  const bo: ShapeModdleElement = {
    $type: "bpmn:Process",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [
        {
          $type: "nano:Shapes",
          shapes: [
            {
              $type: "nano:Shape",
              id: "  Ok  ",
              name: "  ",
              ops: [
                { $type: "nano:Carry" }, // no ref → dropped
                { $type: "nano:Extend", name: "approved" }, // no type → dropped
                { $type: "nano:Project", ref: "Customer" }, // no fields → dropped
                { $type: "nano:Project", ref: "Customer", fields: "  " }, // empty fields → dropped
                { $type: "nano:Carry", ref: "Order" },
              ],
            },
            {
              $type: "nano:Shape",
              ops: [{ $type: "nano:Carry", ref: "Order" }],
            }, // no id → skipped
          ],
        },
      ],
    },
  };
  assert.deepEqual(readShapes(bo), [
    { id: "Ok", ops: [{ op: "carry", ref: "Order" }] },
  ]);
});

test("buildShapesContainer: serialises project fields as a comma list and omits false flags", () => {
  const container = buildShapesContainer(moddle, sample);
  const project = container.shapes?.[0].ops?.find(
    (o) => o.$type === "nano:Project",
  );
  assert.equal(project?.fields, "tier, region");
  assert.equal(project?.via, "Order.customerId");
  const carry = container.shapes?.[0].ops?.find(
    (o) => o.$type === "nano:Carry",
  );
  assert.equal(carry?.spread, undefined);
  const optionalExtend = container.shapes?.[0].ops?.find(
    (o) => o.$type === "nano:Extend" && o.name === "reviewedBy",
  );
  assert.equal(optionalExtend?.optional, true);
});

test("writeShapes: creates the extensionElements + shapes container in one command", () => {
  const bo: ShapeModdleElement = { $type: "bpmn:Process", id: "orders" };
  const modeling = recordingModeling();
  writeShapes(moddle, modeling, {}, bo, sample);
  assert.equal(modeling.calls.length, 1);
  const { moddleElement, props } = modeling.calls[0];
  // No prior extension elements: the target is the process, with a fresh container.
  assert.equal((moddleElement as ShapeModdleElement).$type, "bpmn:Process");
  const ext = props.extensionElements as ShapeModdleElement;
  assert.equal(ext.$type, "bpmn:ExtensionElements");
  assert.equal(ext.values?.length, 1);
  assert.equal(ext.values?.[0].$type, "nano:Shapes");
  // The written container reads back to the original decls.
  assert.deepEqual(readShapes({ ...bo, extensionElements: ext }), sample);
});

test("writeShapes: preserves other extension elements and swaps only the shapes container", () => {
  const meta: ShapeModdleElement = {
    $type: "nano:Meta",
    key: "classification",
    value: "internal",
  };
  const bo: ShapeModdleElement = {
    $type: "bpmn:Process",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [meta, buildShapesContainer(moddle, [{ id: "Old", ops: [] }])],
    },
  };
  const modeling = recordingModeling();
  writeShapes(moddle, modeling, {}, bo, sample);
  const values = modeling.calls[0].props.values as ShapeModdleElement[];
  assert.equal(values.length, 2);
  assert.equal(values[0], meta); // meta preserved
  assert.equal(values[1].$type, "nano:Shapes");
  const nextExt: ShapeModdleElement = {
    $type: "bpmn:ExtensionElements",
    values,
  };
  assert.deepEqual(readShapes({ ...bo, extensionElements: nextExt }), sample);
});

test("writeShapes: replaces the container in place when it is not the last sibling", () => {
  // The `nano:Shapes` container precedes `nano:meta`; a correct in-place swap keeps
  // it at index 0 (the old append-at-end path would reorder to [meta, shapes]).
  const meta: ShapeModdleElement = { $type: "nano:Meta", key: "k", value: "v" };
  const bo: ShapeModdleElement = {
    $type: "bpmn:Process",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [buildShapesContainer(moddle, [{ id: "Old", ops: [] }]), meta],
    },
  };
  const modeling = recordingModeling();
  writeShapes(moddle, modeling, {}, bo, sample);
  const values = modeling.calls[0].props.values as ShapeModdleElement[];
  assert.equal(values.length, 2);
  assert.equal(values[0].$type, "nano:Shapes"); // container stays first
  assert.equal(values[1], meta); // meta stays after it
});

test("writeShapes: an empty shape list removes the container but keeps meta", () => {
  const meta: ShapeModdleElement = { $type: "nano:Meta", key: "k", value: "v" };
  const bo: ShapeModdleElement = {
    $type: "bpmn:Process",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [meta, buildShapesContainer(moddle, [{ id: "Old", ops: [] }])],
    },
  };
  const modeling = recordingModeling();
  writeShapes(moddle, modeling, {}, bo, []);
  const values = modeling.calls[0].props.values as ShapeModdleElement[];
  assert.deepEqual(values, [meta]);
  // The rebuilt extension elements carry no shapes container, so it reads back empty.
  const nextExt: ShapeModdleElement = {
    $type: "bpmn:ExtensionElements",
    values,
  };
  assert.deepEqual(readShapes({ ...bo, extensionElements: nextExt }), []);
});

// Model-level metadata carrier (ADR 0040 §5) ---------------------------------

const sampleMeta: MetaEntry[] = [
  { key: "classification", value: "internal" },
  { key: "owner", value: "ops" },
];

/** A process businessObject carrying `nano:meta` siblings of a shapes container. */
function processWithMeta(
  meta: MetaEntry[],
  shapes: ShapeDecl[] = [],
): ShapeModdleElement {
  const values: ShapeModdleElement[] = [
    ...(shapes.length ? [buildShapesContainer(moddle, shapes)] : []),
    ...meta.map(
      (m) =>
        ({
          $type: "nano:Meta",
          key: m.key,
          value: m.value,
        }) as ShapeModdleElement,
    ),
  ];
  return {
    $type: "bpmn:Process",
    id: "orders",
    extensionElements: { $type: "bpmn:ExtensionElements", values },
  };
}

test("readMeta: reads model-level meta in author order and trims", () => {
  const bo = processWithMeta([
    { key: "  classification  ", value: "  internal  " },
    { key: "owner", value: "ops" },
  ]);
  assert.deepEqual(readMeta(bo), sampleMeta);
});

test("readMeta: returns [] when the process declares no meta", () => {
  assert.deepEqual(readMeta({ $type: "bpmn:Process" }), []);
  assert.deepEqual(readMeta(undefined), []);
  assert.deepEqual(readMeta(processWithShapes(sample)), []);
});

test("readMeta: skips an entry with a blank key", () => {
  const bo = processWithMeta([
    { key: "  ", value: "x" },
    { key: "keep", value: "y" },
  ]);
  assert.deepEqual(readMeta(bo), [{ key: "keep", value: "y" }]);
});

test("writeMeta: creates the extensionElements + meta siblings in one command", () => {
  const bo: ShapeModdleElement = { $type: "bpmn:Process", id: "orders" };
  const modeling = recordingModeling();
  writeMeta(moddle, modeling, {}, bo, sampleMeta);
  assert.equal(modeling.calls.length, 1);
  const ext = modeling.calls[0].props.extensionElements as ShapeModdleElement;
  assert.equal(ext.$type, "bpmn:ExtensionElements");
  assert.deepEqual(readMeta({ ...bo, extensionElements: ext }), sampleMeta);
});

test("writeMeta: preserves the shapes container and swaps only the meta run in place", () => {
  const shapes = buildShapesContainer(moddle, [{ id: "Old", ops: [] }]);
  const bo: ShapeModdleElement = {
    $type: "bpmn:Process",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [shapes, { $type: "nano:Meta", key: "stale", value: "x" }],
    },
  };
  const modeling = recordingModeling();
  writeMeta(moddle, modeling, {}, bo, sampleMeta);
  const values = modeling.calls[0].props.values as ShapeModdleElement[];
  assert.equal(values[0], shapes); // shapes container preserved, still first
  const nextExt: ShapeModdleElement = {
    $type: "bpmn:ExtensionElements",
    values,
  };
  assert.deepEqual(readMeta({ ...bo, extensionElements: nextExt }), sampleMeta);
  assert.deepEqual(readShapes({ ...bo, extensionElements: nextExt }), [
    { id: "Old", ops: [] },
  ]);
});

test("writeMeta: an empty list removes every meta but keeps the shapes container", () => {
  const shapes = buildShapesContainer(moddle, [{ id: "Old", ops: [] }]);
  const bo: ShapeModdleElement = {
    $type: "bpmn:Process",
    extensionElements: {
      $type: "bpmn:ExtensionElements",
      values: [{ $type: "nano:Meta", key: "k", value: "v" }, shapes],
    },
  };
  const modeling = recordingModeling();
  writeMeta(moddle, modeling, {}, bo, []);
  const values = modeling.calls[0].props.values as ShapeModdleElement[];
  assert.deepEqual(values, [shapes]);
});

test("writeMeta: drops entries with a blank key", () => {
  const bo: ShapeModdleElement = { $type: "bpmn:Process" };
  const modeling = recordingModeling();
  writeMeta(moddle, modeling, {}, bo, [
    { key: "  ", value: "x" },
    { key: "keep", value: "y" },
  ]);
  const ext = modeling.calls[0].props.extensionElements as ShapeModdleElement;
  assert.deepEqual(readMeta({ ...bo, extensionElements: ext }), [
    { key: "keep", value: "y" },
  ]);
});

test("writeMeta: trims the value so a round-trip is stable (symmetric with readMeta)", () => {
  const bo: ShapeModdleElement = { $type: "bpmn:Process" };
  const modeling = recordingModeling();
  writeMeta(moddle, modeling, {}, bo, [{ key: "owner", value: "  ops  " }]);
  const ext = modeling.calls[0].props.extensionElements as ShapeModdleElement;
  // The persisted value is trimmed at write time, so re-reading yields the same
  // value (no silent strip on the next read).
  assert.equal((ext.values?.[0] as ShapeModdleElement).value, "ops");
  assert.deepEqual(readMeta({ ...bo, extensionElements: ext }), [
    { key: "owner", value: "ops" },
  ]);
});
