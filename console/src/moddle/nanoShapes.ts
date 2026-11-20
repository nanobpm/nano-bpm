// Nano composed-shape moddle descriptor (ADR 0040 §9) --------------------------
//
// Registered as a bpmn-js moddle extension so the modeler parses and *serialises*
// `nano:shape` declarations as first-class moddle objects (bpmn-js drops extension
// elements it has no descriptor for, so this is what keeps composed shapes in the
// `.bpmn` on save). Composed shapes live in a `nano:shapes` container on the
// defining `bpmn:process`'s `bpmn:extensionElements`; each `nano:shape` carries an
// ordered list of the four composition ops (carry / project / extend / reference),
// collected polymorphically through the abstract `ShapeOp` base so XML order — the
// author-order fold the reifier depends on — is preserved.
//
// `tagAlias: "lowerCase"` lowercases the first letter of each type name, so
// `Shape` serialises as `nano:shape`, `Carry` as `nano:carry`, etc. — matching the
// local names the Rust scan (`server/src/console/envelope_scan.rs`) reads.

/** The nano-shapes moddle descriptor, shaped for bpmn-js `moddleExtensions`. */
export const nanoShapesModdle = {
  name: "Nano",
  uri: "https://nanobpm.io/schema/shapes/1.0",
  prefix: "nano",
  xml: { tagAlias: "lowerCase" },
  types: [
    {
      name: "Shapes",
      superClass: ["Element"],
      properties: [{ name: "shapes", type: "Shape", isMany: true }],
    },
    {
      name: "Shape",
      superClass: ["Element"],
      properties: [
        { name: "id", type: "String", isAttr: true },
        { name: "name", type: "String", isAttr: true },
        { name: "ops", type: "ShapeOp", isMany: true },
      ],
    },
    // Abstract base so the four concrete ops collect into `Shape#ops` in document
    // order (never serialised on its own).
    { name: "ShapeOp", isAbstract: true, superClass: ["Element"] },
    {
      name: "Carry",
      superClass: ["ShapeOp"],
      properties: [{ name: "ref", type: "String", isAttr: true }],
    },
    {
      name: "Project",
      superClass: ["ShapeOp"],
      properties: [
        { name: "ref", type: "String", isAttr: true },
        { name: "fields", type: "String", isAttr: true },
        { name: "via", type: "String", isAttr: true },
      ],
    },
    {
      name: "Extend",
      superClass: ["ShapeOp"],
      properties: [
        { name: "name", type: "String", isAttr: true },
        { name: "type", type: "String", isAttr: true },
        { name: "optional", type: "Boolean", isAttr: true },
        { name: "list", type: "Boolean", isAttr: true },
        { name: "description", type: "String", isAttr: true },
      ],
    },
    {
      name: "Reference",
      superClass: ["ShapeOp"],
      properties: [
        { name: "name", type: "String", isAttr: true },
        { name: "ref", type: "String", isAttr: true },
        { name: "spread", type: "Boolean", isAttr: true },
        { name: "list", type: "Boolean", isAttr: true },
        { name: "description", type: "String", isAttr: true },
      ],
    },
    // Model-level metadata (ADR 0040 §5), a sibling of `nano:shapes` under the
    // process extension elements. Carried for round-trip; not consumed by codegen.
    {
      name: "Meta",
      superClass: ["Element"],
      properties: [
        { name: "key", type: "String", isAttr: true },
        { name: "value", type: "String", isAttr: true },
      ],
    },
  ],
  enumerations: [],
  associations: [],
};

export default nanoShapesModdle;
