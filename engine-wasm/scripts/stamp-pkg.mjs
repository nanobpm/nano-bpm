#!/usr/bin/env node
// Stamp the built @nanobpm/engine-wasm package (engine-wasm/pkg) with its ONE
// version and its build commit (#1871).
//
// One engine version: `server vX.Y.Z` <=> `@nanobpm/engine-wasm X.Y.Z`. The
// version is DERIVED from server/Cargo.toml (the gateway's own version, which
// scripts/release-tags.mjs also derives the `v*` release tag from), never kept
// in a second place: engine-wasm/pkg.package.json carries no version at all.
//
// The stamp also records the commit the wasm was built from in
// `pkg/build-info.json` (exported as `@nanobpm/engine-wasm/build-info`), so the
// console bundle can record which engine it embeds and CI can prove it was
// built from the same commit as the server (scripts/engine-parity.mjs).
//
// Usage (run by `make console-wasm`):
//   node engine-wasm/scripts/stamp-pkg.mjs <repo-root> <pkg-dir>

import { readFileSync, writeFileSync } from "node:fs";
import { execFileSync } from "node:child_process";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";

import { cargoPackageVersion } from "../../scripts/release-tags.mjs";

/** The engine version: the gateway's server/Cargo.toml version (single source). */
export function engineVersion(read) {
  return cargoPackageVersion(read("server/Cargo.toml"));
}

/** The pkg/package.json for a template + version. Fails if the template pins one. */
export function stampManifest(template, version) {
  if ("version" in template) {
    throw new Error(
      "engine-wasm/pkg.package.json must not carry a version: it is derived from server/Cargo.toml (#1871)",
    );
  }
  // Keep `version` right after `name`, where npm tooling expects it.
  const { name, ...rest } = template;
  return { name, version, ...rest };
}

/** The commit the package is built from (`unknown` outside a git checkout). */
export function buildCommit(root) {
  if (process.env.ENGINE_WASM_BUILD_COMMIT) return process.env.ENGINE_WASM_BUILD_COMMIT;
  try {
    return execFileSync("git", ["rev-parse", "HEAD"], { cwd: root, encoding: "utf8" }).trim();
  } catch {
    return "unknown";
  }
}

function main([rootArg, pkgArg]) {
  if (!rootArg || !pkgArg) throw new Error("usage: stamp-pkg.mjs <repo-root> <pkg-dir>");
  const root = resolve(rootArg);
  const pkg = resolve(pkgArg);
  const read = (p) => readFileSync(join(root, p), "utf8");
  const version = engineVersion(read);
  const template = JSON.parse(read("engine-wasm/pkg.package.json"));
  const manifest = stampManifest(template, version);
  writeFileSync(join(pkg, "package.json"), `${JSON.stringify(manifest, null, 2)}\n`);
  const info = { name: manifest.name, version, commit: buildCommit(root) };
  writeFileSync(join(pkg, "build-info.json"), `${JSON.stringify(info, null, 2)}\n`);
  console.log(`stamped ${manifest.name}@${version} (commit ${info.commit})`);
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  try {
    main(process.argv.slice(2));
  } catch (e) {
    console.error(`stamp-pkg: ${e.message}`);
    process.exit(1);
  }
}
