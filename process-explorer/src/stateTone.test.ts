import { test } from "node:test";
import assert from "node:assert/strict";
import { stateTone } from "./stateTone.ts";

test("incident always wins regardless of state", () => {
  assert.equal(stateTone("Active", true), "danger");
  assert.equal(stateTone("Completed", true), "danger");
  assert.equal(stateTone("Terminated", true), "danger");
  assert.equal(stateTone("anything", true), "danger");
});

test("state maps to its tone when there is no incident", () => {
  assert.equal(stateTone("Active", false), "info");
  assert.equal(stateTone("Suspended", false), "warn");
  assert.equal(stateTone("Completed", false), "ok");
  assert.equal(stateTone("Terminated", false), "neutral");
});

test("a suspended instance still reads as danger when it carries an incident", () => {
  // An incident always wins, even over the amber "held" suspended tone.
  assert.equal(stateTone("Suspended", true), "danger");
});

test("unknown states fall back to neutral", () => {
  assert.equal(stateTone("Terminating", false), "neutral");
  assert.equal(stateTone("", false), "neutral");
});
