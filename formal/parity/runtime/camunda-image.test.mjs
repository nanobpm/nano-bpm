import { test } from "node:test";
import assert from "node:assert/strict";
import { latestPatch, minorFromRef } from "./camunda-image.mjs";

test("the image line is derived from the Zeebe pin ref", () => {
  assert.equal(minorFromRef("stable/8.10"), "8.10");
  assert.throws(() => minorFromRef("main"), /cannot derive/);
  assert.throws(() => minorFromRef(undefined), /cannot derive/);
});

test("latestPatch picks the highest released patch, numerically, ignoring pre-releases", () => {
  const tags = [
    "8.10.9",
    "8.10.10",
    "8.10.11-rc1",
    "8.10-SNAPSHOT",
    "8.10.2-alpha1",
    "8.1.99",
    "8.100.1",
  ];
  assert.equal(latestPatch("8.10", tags), "8.10.10");
});

test("latestPatch fails loudly when the pinned line has no release", () => {
  assert.throws(() => latestPatch("8.12", ["8.12-SNAPSHOT", "8.12.0-alpha1"]), /no released/);
});
