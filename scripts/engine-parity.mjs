#!/usr/bin/env node
// Guard (#1871): the console's embedded studio test engine and the server
// engine come from the SAME commit and the SAME version.
//
// One engine version: `server vX.Y.Z` <=> `@nanobpm/engine-wasm X.Y.Z`, both
// derived from server/Cargo.toml. engine-wasm/scripts/stamp-pkg.mjs records the
// version and build commit in `engine-wasm/pkg/build-info.json`; the console
// build copies it into its bundle as `dist/engine-wasm.json`. This compares a
// build-info record against the server's version and the commit being built.
//
// The failure mode it kills: the console took `@nanobpm/engine-wasm` from an
// npm range, so the studio tested against an older engine than the server it
// shipped in (nano-workforce#1045: the #1717 output-scope change was live in the
// server while the wasm tests still ran 0.11.1 and passed).
//
// Usage:
//   node scripts/engine-parity.mjs <build-info.json>... [--commit <sha>]
// e.g. engine-wasm/pkg/build-info.json, console/dist/engine-wasm.json,
//      console/node_modules/@nanobpm/engine-wasm/build-info.json
// The expected commit defaults to `git rev-parse HEAD`.

import { readFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { cargoPackageVersion } from "./release-tags.mjs";

/** The console's engine-wasm dependency spec: the same-commit pkg build. */
export const CONSOLE_ENGINE_DEP = "file:../engine-wasm/pkg";

/** Every mismatch between a build-info record and the expected server engine. */
export function parityErrors(info, { version, commit }) {
  const errors = [];
  if (info?.name !== "@nanobpm/engine-wasm") errors.push(`name is ${info?.name}, expected @nanobpm/engine-wasm`);
  if (info?.version !== version) errors.push(`engine version ${info?.version} != server version ${version}`);
  if (info?.commit !== commit) errors.push(`engine built from commit ${info?.commit} != server commit ${commit}`);
  return errors;
}

/** Errors if the console does not consume the same-commit engine build. */
export function consoleDepErrors(consolePackageJson) {
  const deps = consolePackageJson.dependencies ?? {};
  const spec = deps["@nanobpm/engine-wasm"];
  const errors = [];
  if (spec !== CONSOLE_ENGINE_DEP) {
    errors.push(`console depends on @nanobpm/engine-wasm "${spec}", expected "${CONSOLE_ENGINE_DEP}" (same-commit build)`);
  }
  // Transitive consumers (the published bojtos packages) must not pull a second,
  // npm-released engine into the bundle.
  if (consolePackageJson.overrides?.["@nanobpm/engine-wasm"] !== "$@nanobpm/engine-wasm") {
    errors.push('console must override @nanobpm/engine-wasm to "$@nanobpm/engine-wasm" so no npm-released copy is bundled');
  }
  return errors;
}

function main(argv) {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const files = [];
  let commit = null;
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--commit") commit = argv[++i];
    else files.push(argv[i]);
  }
  if (!files.length) throw new Error("usage: engine-parity.mjs <build-info.json>... [--commit <sha>]");
  commit ??= execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
  const version = cargoPackageVersion(readFileSync(join(root, "server/Cargo.toml"), "utf8"));
  let failed = false;
  for (const f of files) {
    const errors = parityErrors(JSON.parse(readFileSync(f, "utf8")), { version, commit });
    for (const e of errors) console.error(`::error::${f}: ${e}`);
    if (errors.length) failed = true;
    else console.log(`${f}: @nanobpm/engine-wasm ${version} @ ${commit} — matches the server`);
  }
  if (failed) process.exit(1);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    main(process.argv.slice(2));
  } catch (e) {
    console.error(`engine-parity: ${e.message}`);
    process.exit(1);
  }
}
