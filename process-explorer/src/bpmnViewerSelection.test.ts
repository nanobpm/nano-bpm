import { test } from "node:test";
import assert from "node:assert/strict";
import { selectedElementId } from "./bpmnViewerSelection.ts";

test("selectedElementId returns the id of a clicked shape", () => {
  assert.equal(
    selectedElementId({ id: "CallActivity_1", type: "bpmn:CallActivity" }),
    "CallActivity_1",
  );
});

test("selectedElementId returns null for the diagram root (empty canvas click)", () => {
  assert.equal(
    selectedElementId({ id: "Process_1", type: "bpmn:Process" }),
    null,
  );
  assert.equal(
    selectedElementId({ id: "Collaboration_1", type: "bpmn:Collaboration" }),
    null,
  );
  assert.equal(
    selectedElementId({ id: "Definitions_1", type: "bpmn:Definitions" }),
    null,
  );
});

test("selectedElementId returns null for a missing element or missing id", () => {
  assert.equal(selectedElementId(null), null);
  assert.equal(selectedElementId(undefined), null);
  assert.equal(selectedElementId({ type: "bpmn:Task" }), null);
});

test("selectedElementId resolves a label click to the labelled element", () => {
  assert.equal(
    selectedElementId({
      id: "Flow_1_label",
      type: "label",
      labelTarget: { id: "Flow_1", type: "bpmn:SequenceFlow" },
    }),
    "Flow_1",
  );
});

test("selectedElementId ignores a label whose target is the root", () => {
  assert.equal(
    selectedElementId({
      id: "Process_1_label",
      type: "label",
      labelTarget: { id: "Process_1", type: "bpmn:Process" },
    }),
    null,
  );
});
