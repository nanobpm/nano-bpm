import { test } from "node:test";
import assert from "node:assert/strict";
import {
  VALUE_JSON_ERROR,
  parseVariableJson,
  scopeKeyOptions,
  validateNewVariable,
  variableExists,
} from "./newVariableForm.ts";

const vars = (
  entries: Array<[name: string, scope: string]>,
): Array<{ name: string; value: string; scope_key: string }> =>
  entries.map(([name, scope_key]) => ({ name, value: "1", scope_key }));

test("parseVariableJson accepts scalars, objects and arrays", () => {
  assert.deepEqual(parseVariableJson("42"), { ok: true, value: 42 });
  assert.deepEqual(parseVariableJson("true"), { ok: true, value: true });
  assert.deepEqual(parseVariableJson('"text"'), { ok: true, value: "text" });
  assert.deepEqual(parseVariableJson('{"a":1}'), {
    ok: true,
    value: { a: 1 },
  });
});

test("parseVariableJson rejects invalid JSON with the shared error", () => {
  assert.deepEqual(parseVariableJson("text"), {
    ok: false,
    error: VALUE_JSON_ERROR,
  });
  assert.deepEqual(parseVariableJson(""), {
    ok: false,
    error: VALUE_JSON_ERROR,
  });
});

test("scopeKeyOptions always leads with the instance scope, even with no vars", () => {
  assert.deepEqual(scopeKeyOptions("100", []), ["100"]);
});

test("scopeKeyOptions appends distinct non-instance scopes, sorted", () => {
  const options = scopeKeyOptions(
    "100",
    vars([
      ["a", "100"],
      ["b", "300"],
      ["c", "200"],
      ["d", "200"],
      ["e", "100"],
    ]),
  );
  assert.deepEqual(options, ["100", "200", "300"]);
});

test("scopeKeyOptions orders non-instance scopes numerically, not lexically", () => {
  const options = scopeKeyOptions(
    "100",
    vars([
      ["a", "2"],
      ["b", "10"],
      ["c", "30"],
    ]),
  );
  assert.deepEqual(options, ["100", "2", "10", "30"]);
});

test("variableExists is scope-qualified", () => {
  const v = vars([
    ["greeting", "100"],
    ["count", "200"],
  ]);
  assert.equal(variableExists(v, "100", "greeting"), true);
  assert.equal(variableExists(v, "200", "greeting"), false);
  assert.equal(variableExists(v, "200", "count"), true);
  assert.equal(variableExists(v, "100", "count"), false);
});

test("validateNewVariable rejects a blank name before anything else", () => {
  const result = validateNewVariable({
    name: "   ",
    valueDraft: "nonsense",
    scopeKey: "100",
    variables: [],
  });
  assert.deepEqual(result, { ok: false, error: "Name is required." });
});

test("validateNewVariable rejects a duplicate name on the chosen scope", () => {
  const result = validateNewVariable({
    name: "greeting",
    valueDraft: '"hi"',
    scopeKey: "100",
    variables: vars([["greeting", "100"]]),
  });
  assert.equal(result.ok, false);
  assert.match(
    (result as { error: string }).error,
    /already exists on this scope/,
  );
});

test("validateNewVariable allows the same name on a different scope", () => {
  const result = validateNewVariable({
    name: "greeting",
    valueDraft: '"hi"',
    scopeKey: "200",
    variables: vars([["greeting", "100"]]),
  });
  assert.deepEqual(result, {
    ok: true,
    name: "greeting",
    value: "hi",
    scopeKey: "200",
  });
});

test("validateNewVariable rejects an invalid JSON value", () => {
  const result = validateNewVariable({
    name: "greeting",
    valueDraft: "not json",
    scopeKey: "100",
    variables: [],
  });
  assert.deepEqual(result, { ok: false, error: VALUE_JSON_ERROR });
});

test("validateNewVariable trims the name and returns the parsed value", () => {
  const result = validateNewVariable({
    name: "  count  ",
    valueDraft: "42",
    scopeKey: "100",
    variables: [],
  });
  assert.deepEqual(result, {
    ok: true,
    name: "count",
    value: 42,
    scopeKey: "100",
  });
});
