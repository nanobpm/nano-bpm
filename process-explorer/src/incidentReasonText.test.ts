// Unit tests for the incident-reason display helpers. Node-native: run with
// `node --experimental-strip-types --test src/lib/incidentReason.test.ts`.
import { test } from "node:test";
import assert from "node:assert/strict";
import {
  countReasonLines,
  isMultilineReason,
  shouldCollapseReason,
  REASON_COLLAPSE_LINES,
  REASON_COLLAPSE_CHARS,
} from "./incidentReasonText.ts";

test("countReasonLines counts newline-delimited lines; empty is zero", () => {
  assert.equal(countReasonLines(""), 0);
  assert.equal(countReasonLines("one line"), 1);
  assert.equal(countReasonLines("a\nb\nc"), 3);
  // A trailing newline yields an empty final line — still counted, matching how
  // it renders (a blank last row).
  assert.equal(countReasonLines("a\n"), 2);
});

test("isMultilineReason detects an embedded newline", () => {
  assert.equal(isMultilineReason("single line error"), false);
  assert.equal(
    isMultilineReason("fatal: could not read\nUsername for 'x'"),
    true,
  );
});

test("shouldCollapseReason is false for short single-line reasons", () => {
  assert.equal(shouldCollapseReason("Job worker timed out"), false);
});

test("shouldCollapseReason is true once the reason exceeds the line clamp", () => {
  const reason = Array.from(
    { length: REASON_COLLAPSE_LINES + 1 },
    (_, i) => `line ${i}`,
  ).join("\n");
  assert.equal(shouldCollapseReason(reason), true);
});

test("shouldCollapseReason is false at exactly the line-clamp boundary", () => {
  // Guards an off-by-one that would show a toggle that reveals nothing.
  const reason = Array.from(
    { length: REASON_COLLAPSE_LINES },
    (_, i) => `l${i}`,
  ).join("\n");
  assert.equal(shouldCollapseReason(reason), false);
});

test("shouldCollapseReason is true for a long single run-on line", () => {
  // A worker can emit one un-wrapped line longer than the terminal — it wraps to
  // many visual rows, so it too deserves the clamp even with no newline.
  const reason = "x".repeat(REASON_COLLAPSE_CHARS + 1);
  assert.equal(isMultilineReason(reason), false);
  assert.equal(shouldCollapseReason(reason), true);
});

test("shouldCollapseReason honours caller-supplied bounds", () => {
  assert.equal(shouldCollapseReason("a\nb", 1), true);
  assert.equal(shouldCollapseReason("abcdef", REASON_COLLAPSE_LINES, 3), true);
});
