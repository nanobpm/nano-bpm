// Parity + behaviour tests for the Node ESM loader's `.jsonc` support.
//
// `node_loader.mjs` advertises `node.jsonc`/`deno.jsonc` as import-map candidates,
// so it must accept a *real* JSONC body (comments + trailing commas) — otherwise
// it silently skips the file and the project's imports never resolve. The Rust
// dependency derivation (`strip_jsonc` in projects.rs) reads the same files with
// the same rules; these cases are the shared contract.
import { strict as assert } from "node:assert";
import { test } from "node:test";

// @ts-expect-error -- an implementation module (JS), not a typed one; the import
// exists so the two readers are exercised against the same fixtures.
import { stripJsonc } from "./node_loader.mjs";

test("stripJsonc drops line and block comments", () => {
  const jsonc = `{
    // the import map
    "imports": {
      "@nanobpm/worker": "./sdk/worker-sdk.ts", /* the SDK */
      "@lib/": "./lib/"
    }
  }`;
  const parsed = JSON.parse(stripJsonc(jsonc));
  assert.equal(parsed.imports["@nanobpm/worker"], "./sdk/worker-sdk.ts");
  assert.equal(parsed.imports["@lib/"], "./lib/");
});

test("stripJsonc drops trailing commas", () => {
  const parsed = JSON.parse(stripJsonc(`{ "imports": { "a": "b", }, }`));
  assert.deepEqual(parsed.imports, { a: "b" });
});

test("stripJsonc preserves a // inside a string value", () => {
  const parsed = JSON.parse(stripJsonc(`{ "u": "https://host/x", "c": "a/*b*/c" }`));
  assert.equal(parsed.u, "https://host/x");
  assert.equal(parsed.c, "a/*b*/c");
});

test("stripJsonc handles an escaped quote inside a string", () => {
  const parsed = JSON.parse(stripJsonc(`{ "s": "a\\"//b", "n": 1 }`));
  assert.equal(parsed.s, 'a"//b');
  assert.equal(parsed.n, 1);
});

test("stripJsonc leaves strict JSON unchanged", () => {
  const json = `{"imports":{"a":"b"},"n":[1,2]}`;
  assert.deepEqual(JSON.parse(stripJsonc(json)), JSON.parse(json));
});