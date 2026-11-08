import { test } from "node:test";
import assert from "node:assert/strict";
import {
  callActivityElementIds,
  calledInstancesForElement,
  groupCalledInstances,
  resolveCallActivitySelection,
} from "./calledInstances.ts";
import type { CalledInstance } from "./gen";

/** Build a `CalledInstance` row with sensible defaults for the fields a test
 * doesn't care about. */
function called(
  over: Partial<CalledInstance> & { key: string },
): CalledInstance {
  return {
    key: over.key,
    process_id: over.process_id ?? "child-proc",
    version: over.version ?? 1,
    state: over.state ?? "Active",
    has_incident: over.has_incident ?? false,
    start_date_ms: over.start_date_ms ?? 0,
    calling_element_id: over.calling_element_id ?? null,
    calling_element_name: over.calling_element_name ?? null,
  };
}

const XML = `<?xml version="1.0"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="parent">
    <bpmn:startEvent id="start" />
    <bpmn:callActivity id="Call_A" name="Call A" calledElement="child-a" />
    <bpmn:serviceTask id="Task_1" />
    <callActivity name="Call B" id="Call_B" />
    <bpmn:endEvent id="end" />
  </bpmn:process>
</bpmn:definitions>`;

test("callActivityElementIds extracts namespaced and bare callActivity ids (any attr order)", () => {
  const ids = callActivityElementIds(XML);
  assert.deepEqual([...ids].sort(), ["Call_A", "Call_B"]);
});

test("callActivityElementIds returns an empty set for null/empty xml", () => {
  assert.equal(callActivityElementIds(null).size, 0);
  assert.equal(callActivityElementIds("").size, 0);
  assert.equal(callActivityElementIds("<bpmn:process id='p'/>").size, 0);
});

test("calledInstancesForElement filters by calling_element_id", () => {
  const rows = [
    called({ key: "c1", calling_element_id: "Call_A" }),
    called({ key: "c2", calling_element_id: "Call_B" }),
    called({ key: "c3", calling_element_id: "Call_A" }),
  ];
  assert.deepEqual(
    calledInstancesForElement("Call_A", rows).map((r) => r.key),
    ["c1", "c3"],
  );
});

test("groupCalledInstances groups by calling cell preserving order; MI cell yields one group of N", () => {
  const rows = [
    called({
      key: "c1",
      calling_element_id: "Call_A",
      calling_element_name: "Call A",
    }),
    called({
      key: "c2",
      calling_element_id: "Call_MI",
      calling_element_name: "Fan out",
    }),
    called({
      key: "c3",
      calling_element_id: "Call_MI",
      calling_element_name: "Fan out",
    }),
    called({
      key: "c4",
      calling_element_id: "Call_MI",
      calling_element_name: "Fan out",
    }),
  ];
  const groups = groupCalledInstances(rows);
  assert.equal(groups.length, 2);
  assert.deepEqual(groups[0], {
    elementId: "Call_A",
    elementName: "Call A",
    instances: [rows[0]],
  });
  assert.equal(groups[1].elementId, "Call_MI");
  assert.equal(groups[1].elementName, "Fan out");
  assert.deepEqual(
    groups[1].instances.map((i) => i.key),
    ["c2", "c3", "c4"],
  );
});

test("groupCalledInstances keeps unresolved-cell rows under a null group (never dropped)", () => {
  const rows = [called({ key: "c1", calling_element_id: null })];
  const groups = groupCalledInstances(rows);
  assert.equal(groups.length, 1);
  assert.equal(groups[0].elementId, null);
  assert.deepEqual(
    groups[0].instances.map((i) => i.key),
    ["c1"],
  );
});

test("resolveCallActivitySelection: exactly one child -> navigate straight to it", () => {
  const rows = [called({ key: "child-1", calling_element_id: "Call_A" })];
  const sel = resolveCallActivitySelection("Call_A", rows, new Set(["Call_A"]));
  assert.deepEqual(sel, { kind: "navigate", instanceKey: "child-1" });
});

test("resolveCallActivitySelection: multiple children -> reveal that cell's rows", () => {
  const rows = [
    called({ key: "c1", calling_element_id: "Call_MI" }),
    called({ key: "c2", calling_element_id: "Call_MI" }),
  ];
  const sel = resolveCallActivitySelection(
    "Call_MI",
    rows,
    new Set(["Call_MI"]),
  );
  assert.deepEqual(sel, { kind: "reveal", elementId: "Call_MI" });
});

test("resolveCallActivitySelection: call activity with no child yet -> none affordance", () => {
  const sel = resolveCallActivitySelection("Call_A", [], new Set(["Call_A"]));
  assert.deepEqual(sel, { kind: "none", elementId: "Call_A" });
});

test("resolveCallActivitySelection: non-call-activity element -> ignore (no navigation)", () => {
  const rows = [called({ key: "c1", calling_element_id: "Call_A" })];
  const sel = resolveCallActivitySelection("Task_1", rows, new Set(["Call_A"]));
  assert.deepEqual(sel, { kind: "ignore" });
});

test("resolveCallActivitySelection: zero matches and unknown call-activity set -> ignore (fail safe)", () => {
  const sel = resolveCallActivitySelection("Call_A", []);
  assert.deepEqual(sel, { kind: "ignore" });
});
