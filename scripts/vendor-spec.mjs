#!/usr/bin/env node
// Vendors the Camunda Orchestration Cluster REST spec into spec/ and guards it
// (#1291). spec/ must stay byte-identical to the upstream tree pinned in
// spec-patches/upstream.json - every local change goes through
// spec-patches/patches.yaml (applied at build time by preprocess-spec.py).
//
//   node scripts/vendor-spec.mjs --check     # verify spec/ == pinned upstream (CI)
//   node scripts/vendor-spec.mjs <commit>    # re-vendor at <commit>, update the pin
//
// After re-vendoring: `make generate`, then re-triage new request fields with
// UPDATE_REQUEST_FIELDS=1 (server/src/request_field_guard.rs).

import { execFileSync } from "node:child_process";
import {
  cpSync,
  existsSync,
  mkdtempSync,
  readdirSync,
  readFileSync,
  rmSync,
  writeFileSync,
} from "node:fs";
import { tmpdir } from "node:os";
import { dirname, join, relative, resolve } from "node:path";
import { fileURLToPath } from "node:url";

export const ROOT = resolve(dirname(fileURLToPath(import.meta.url)), "..");
export const PIN_FILE = "spec-patches/upstream.json";
export const SPEC_DIR = "spec";
/**
 * The only upstream `spec/` may be vendored from (#1291). Enforced by
 * `readPin`, so editing the pin cannot make `--check` bless another tree.
 */
export const UPSTREAM_REPOSITORY = "https://github.com/camunda/camunda.git";
export const UPSTREAM_PATH = "zeebe/gateway-protocol/src/main/proto/v2";

export function readPin(root = ROOT) {
  const pin = JSON.parse(readFileSync(join(root, PIN_FILE), "utf8"));
  for (const key of ["repository", "commit", "path"]) {
    if (typeof pin[key] !== "string" || pin[key] === "") {
      throw new Error(`${PIN_FILE}: missing "${key}"`);
    }
  }
  if (!/^[0-9a-f]{40}$/.test(pin.commit)) {
    throw new Error(`${PIN_FILE}: "commit" must be a full 40-hex SHA, got ${pin.commit}`);
  }
  if (pin.repository !== UPSTREAM_REPOSITORY || pin.path !== UPSTREAM_PATH) {
    throw new Error(
      `${PIN_FILE}: spec/ must be vendored from ${UPSTREAM_REPOSITORY} (${UPSTREAM_PATH}), ` +
        `got ${pin.repository} (${pin.path})`,
    );
  }
  return pin;
}

/** Relative paths of every file under `dir`, sorted. */
export function listFiles(dir) {
  const out = [];
  const walk = (d) => {
    for (const entry of readdirSync(d, { withFileTypes: true })) {
      const p = join(d, entry.name);
      if (entry.isDirectory()) walk(p);
      else out.push(relative(dir, p).split("\\").join("/"));
    }
  };
  walk(dir);
  return out.sort();
}

/**
 * Byte-compares two trees. `missing`: upstream files absent locally;
 * `extra`: local files upstream does not have; `changed`: differing bytes.
 */
export function compareTrees(upstreamDir, specDir) {
  const up = listFiles(upstreamDir);
  const local = listFiles(specDir);
  const localSet = new Set(local);
  const upSet = new Set(up);
  return {
    missing: up.filter((f) => !localSet.has(f)),
    extra: local.filter((f) => !upSet.has(f)),
    changed: up.filter(
      (f) =>
        localSet.has(f) &&
        !readFileSync(join(upstreamDir, f)).equals(readFileSync(join(specDir, f))),
    ),
  };
}

export function isClean({ missing, extra, changed }) {
  return missing.length === 0 && extra.length === 0 && changed.length === 0;
}

/**
 * Shallow, blob-filtered, sparse fetch of `pin.path` at `commit`; returns the
 * directory holding that subtree. Cheap even for camunda/camunda.
 */
export function fetchUpstream(pin, commit, workDir) {
  const git = (...args) => execFileSync("git", ["-C", workDir, ...args], { stdio: "inherit" });
  execFileSync("git", ["init", "-q", workDir], { stdio: "inherit" });
  git("remote", "add", "origin", pin.repository);
  git("sparse-checkout", "set", "--no-cone", `/${pin.path}/`);
  git("fetch", "-q", "--depth=1", "--filter=blob:none", "origin", commit);
  git("-c", "advice.detachedHead=false", "checkout", "-q", "FETCH_HEAD");
  const tree = join(workDir, pin.path);
  if (!existsSync(tree)) throw new Error(`${pin.path} does not exist at ${commit}`);
  return tree;
}

export function formatDrift({ missing, extra, changed }) {
  const lines = [];
  for (const f of missing) lines.push(`  missing locally: ${SPEC_DIR}/${f}`);
  for (const f of extra) lines.push(`  not upstream:    ${SPEC_DIR}/${f}`);
  for (const f of changed) lines.push(`  modified:        ${SPEC_DIR}/${f}`);
  return lines.join("\n");
}

function main(argv) {
  const pin = readPin();
  const arg = argv[0];
  if (!arg || argv.length > 1) {
    console.error("usage: vendor-spec.mjs --check | <commit>");
    return 2;
  }
  const work = mkdtempSync(join(tmpdir(), "nano-spec-"));
  try {
    if (arg === "--check") {
      const drift = compareTrees(fetchUpstream(pin, pin.commit, work), join(ROOT, SPEC_DIR));
      if (!isClean(drift)) {
        console.error(
          `${SPEC_DIR}/ differs from ${pin.repository} ${pin.commit} (${pin.path}):\n` +
            `${formatDrift(drift)}\n` +
            `Put local changes in spec-patches/patches.yaml, or re-vendor with ` +
            `\`node scripts/vendor-spec.mjs <commit>\`.`,
        );
        return 1;
      }
      console.log(`${SPEC_DIR}/ matches ${pin.repository} @ ${pin.commit}`);
      return 0;
    }
    if (!/^[0-9a-f]{40}$/.test(arg)) {
      console.error(`expected a full 40-hex commit SHA, got ${arg}`);
      return 2;
    }
    const tree = fetchUpstream(pin, arg, work);
    rmSync(join(ROOT, SPEC_DIR), { recursive: true, force: true });
    cpSync(tree, join(ROOT, SPEC_DIR), { recursive: true });
    const raw = JSON.parse(readFileSync(join(ROOT, PIN_FILE), "utf8"));
    writeFileSync(join(ROOT, PIN_FILE), `${JSON.stringify({ ...raw, commit: arg }, null, 2)}\n`);
    console.log(
      `Vendored ${pin.path} @ ${arg}. Next: make generate, then triage new request ` +
        `fields (UPDATE_REQUEST_FIELDS=1, server/src/request_field_guard.rs).`,
    );
    return 0;
  } finally {
    rmSync(work, { recursive: true, force: true });
  }
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  process.exitCode = main(process.argv.slice(2));
}
