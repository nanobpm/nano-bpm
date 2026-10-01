import { test } from "node:test";
import assert from "node:assert/strict";
import { selectPaths, type OpenApiDoc } from "./specSubset.ts";

const doc: OpenApiDoc = {
  openapi: "3.0.3",
  paths: {
    "/a": {
      post: {
        requestBody: {
          content: {
            "application/json": { schema: { $ref: "#/components/schemas/A" } },
          },
        },
        responses: { "400": { $ref: "#/components/responses/Bad" } },
      },
    },
    "/b": { get: { $ref: "#/components/schemas/B" } },
  },
  components: {
    schemas: {
      A: { properties: { k: { $ref: "#/components/schemas/Key" } } },
      Key: { type: "string" },
      B: { type: "object" },
      "Odd/Name": { type: "string" },
      Cycle: { properties: { self: { $ref: "#/components/schemas/Cycle" } } },
    },
    responses: { Bad: { description: "bad" }, Unused: { description: "x" } },
  },
};

test("keeps only the requested paths and their transitive components", () => {
  const out = selectPaths(doc, ["/a"]);
  assert.deepEqual(Object.keys(out.paths ?? {}), ["/a"]);
  assert.deepEqual(Object.keys(out.components?.schemas ?? {}).sort(), [
    "A",
    "Key",
  ]);
  assert.deepEqual(Object.keys(out.components?.responses ?? {}), ["Bad"]);
  assert.equal(out.openapi, "3.0.3");
});

test("follows escaped pointers and terminates on cycles", () => {
  const cyclic: OpenApiDoc = {
    paths: {
      "/c": {
        get: {
          x: { $ref: "#/components/schemas/Cycle" },
          y: { $ref: "#/components/schemas/Odd~1Name" },
        },
      },
    },
    components: doc.components,
  };
  const out = selectPaths(cyclic, ["/c"]);
  assert.deepEqual(Object.keys(out.components?.schemas ?? {}).sort(), [
    "Cycle",
    "Odd/Name",
  ]);
});

test("a missing path fails loudly instead of generating nothing", () => {
  assert.throws(
    () => selectPaths(doc, ["/gone"]),
    /\/gone is not in the source spec/,
  );
});

test("does not mutate the input document", () => {
  const before = JSON.stringify(doc);
  selectPaths(doc, ["/a"]);
  assert.equal(JSON.stringify(doc), before);
});
