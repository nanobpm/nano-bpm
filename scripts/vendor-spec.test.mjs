// Tests for scripts/vendor-spec.mjs - the vendored-spec drift guard (#1291).
// Run: node --test scripts/vendor-spec.test.mjs

import { test } from "node:test";
import assert from "node:assert/strict";
import { mkdirSync, mkdtempSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import {
  compareTrees,
  formatDrift,
  isClean,
  PIN_FILE,
  readPin,
  ROOT,
  UPSTREAM_PATH,
  UPSTREAM_REPOSITORY,
} from "./vendor-spec.mjs";

function tree(files) {
  const dir = mkdtempSync(join(tmpdir(), "vendor-spec-test-"));
  for (const [path, content] of Object.entries(files)) {
    mkdirSync(dirname(join(dir, path)), { recursive: true });
    writeFileSync(join(dir, path), content);
  }
  return dir;
}

const upstream = { "rest-api.yaml": "openapi: 3.0.3\n", "sub/jobs.yaml": "a: 1\n" };

test("identical trees are clean", () => {
  const drift = compareTrees(tree(upstream), tree(upstream));
  assert.ok(isClean(drift), formatDrift(drift));
});

test("a hand-edited, missing, or extra spec file is drift", () => {
  const local = tree({ "rest-api.yaml": "openapi: 3.0.3\n", "sub/jobs.yaml": "a: 2\n", "local.yaml": "x\n" });
  const drift = compareTrees(tree({ ...upstream, "sub/new.yaml": "n\n" }), local);
  assert.deepEqual(drift, {
    missing: ["sub/new.yaml"],
    extra: ["local.yaml"],
    changed: ["sub/jobs.yaml"],
  });
  assert.ok(!isClean(drift));
  assert.match(formatDrift(drift), /modified: +spec\/sub\/jobs.yaml/);
});

test("a whitespace-only edit is drift (byte comparison)", () => {
  const drift = compareTrees(tree(upstream), tree({ ...upstream, "rest-api.yaml": "openapi: 3.0.3 \n" }));
  assert.deepEqual(drift.changed, ["rest-api.yaml"]);
});

test("the committed pin is a full SHA of the camunda spec path", () => {
  const pin = readPin();
  assert.match(pin.commit, /^[0-9a-f]{40}$/);
  assert.equal(pin.repository, "https://github.com/camunda/camunda.git");
  assert.equal(pin.path, "zeebe/gateway-protocol/src/main/proto/v2");
});

test("a pin naming any other repository or subtree is rejected", () => {
  const sha = "a".repeat(40);
  const pinned = (over) =>
    tree({
      [PIN_FILE]: JSON.stringify({
        repository: UPSTREAM_REPOSITORY,
        commit: sha,
        path: UPSTREAM_PATH,
        ...over,
      }),
    });
  assert.equal(readPin(pinned({})).repository, UPSTREAM_REPOSITORY);
  for (const over of [
    { repository: "https://github.com/someone/camunda-fork.git" },
    { repository: "https://github.com/camunda/camunda" },
    { path: "zeebe/gateway-protocol/src/main/proto" },
  ]) {
    assert.throws(() => readPin(pinned(over)), /must be vendored from/, JSON.stringify(over));
  }
});

test("a malformed pin is rejected", () => {
  const root = tree({
    [PIN_FILE]: JSON.stringify({ repository: "r", commit: "main", path: "p" }),
  });
  assert.throws(() => readPin(root), /full 40-hex SHA/);
  const noPath = tree({ [PIN_FILE]: JSON.stringify({ repository: "r", commit: "a".repeat(40) }) });
  assert.throws(() => readPin(noPath), /missing "path"/);
});

test("ROOT points at the repository", () => {
  assert.ok(readPin(ROOT));
});
