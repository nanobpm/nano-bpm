// Tests for the one-engine-version guard (#1871): scripts/engine-parity.mjs and
// the stamp that feeds it, engine-wasm/scripts/stamp-pkg.mjs.
// Run: node --test scripts/engine-parity.test.mjs

import { test } from "node:test";
import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { CONSOLE_ENGINE_DEP, consoleDepErrors, parityErrors } from "./engine-parity.mjs";
import { engineVersion, stampManifest } from "../engine-wasm/scripts/stamp-pkg.mjs";
import { cargoPackageVersion } from "./release-tags.mjs";

const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
const readRepo = (p) => readFileSync(join(root, p), "utf8");

const server = { version: "1.2.3", commit: "abc123" };
const info = { name: "@nanobpm/engine-wasm", version: "1.2.3", commit: "abc123" };

test("a same-version, same-commit engine passes", () => {
  assert.deepEqual(parityErrors(info, server), []);
});

test("an engine at another version fails (the nano-workforce#1045 drift)", () => {
  const errors = parityErrors({ ...info, version: "0.11.1" }, server);
  assert.equal(errors.length, 1);
  assert.match(errors[0], /engine version 0\.11\.1 != server version 1\.2\.3/);
});

test("an engine built from another commit fails, even at the same version", () => {
  assert.match(parityErrors({ ...info, commit: "def456" }, server).join(), /commit def456 != server commit abc123/);
});

test("an unstamped engine (no build info) fails", () => {
  assert.equal(parityErrors({}, server).length, 3);
});

test("the stamp derives the engine version from server/Cargo.toml", () => {
  const read = (p) => {
    assert.equal(p, "server/Cargo.toml");
    return '[package]\nname = "x"\nversion = "4.5.6"\n[dependencies]\ny = { version = "9" }\n';
  };
  assert.equal(engineVersion(read), "4.5.6");
  const stamped = stampManifest({ name: "@nanobpm/engine-wasm", type: "module" }, "4.5.6");
  assert.deepEqual(Object.keys(stamped).slice(0, 2), ["name", "version"]);
  assert.equal(stamped.version, "4.5.6");
});

test("the stamp refuses a template that pins its own version (a second source of truth)", () => {
  assert.throws(() => stampManifest({ name: "@nanobpm/engine-wasm", version: "0.11.3" }, "1.0.0"), /must not carry a version/);
});

// --- drift guards against the real repo ---------------------------------------

test("the real engine-wasm template carries no version and exports build-info", () => {
  const template = JSON.parse(readRepo("engine-wasm/pkg.package.json"));
  assert.ok(!("version" in template), "engine-wasm/pkg.package.json must not carry a version");
  assert.equal(template.exports["./build-info"], "./build-info.json");
  assert.ok(template.files.includes("build-info.json"));
  assert.equal(
    engineVersion(readRepo),
    cargoPackageVersion(readRepo("server/Cargo.toml")),
  );
});

test("the real console consumes the same-commit engine build, never an npm range", () => {
  const pkg = JSON.parse(readRepo("console/package.json"));
  assert.deepEqual(consoleDepErrors(pkg), []);
  assert.equal(pkg.dependencies["@nanobpm/engine-wasm"], CONSOLE_ENGINE_DEP);
});

test("the console dep guard rejects an npm range and a missing override", () => {
  const errors = consoleDepErrors({ dependencies: { "@nanobpm/engine-wasm": "^0.11.2" } });
  assert.equal(errors.length, 2);
});
