// Guard (#1461): a required gate must not depend on a live GitHub Releases
// download. A Releases 504 failed `formal (tlc)` (wasm-pack's binaryen fetch
// for wasm-opt) and `processos (clippy + test)` (libduckdb-sys' build.rs
// fetch) on a PR whose code was fine. Each of those downloads now goes
// through a cached composite action; this test fails if a workflow job or a
// composite action reintroduces the uncached path.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync, readdirSync, existsSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { duckdbVersion, lockedCrateVersion } from './libduckdb-version.mjs';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

/** Strip YAML/shell comment lines so prose mentioning a command never counts. */
const code = (text) => text.split('\n').filter((l) => !/^\s*#/.test(l)).join('\n');

/**
 * Split a workflow into its jobs (`jobs:` children at two-space indent). A
 * composite action is a single unit.
 */
export function units(name, text) {
  const jobs = text.split(/^jobs:\s*$/m)[1];
  if (jobs === undefined) return [{ name, body: code(text) }];
  return jobs
    .split(/^(?= {2}[\w-]+:\s*$)/m)
    .filter((b) => /^ {2}[\w-]+:/.test(b))
    .map((b) => ({ name: `${name} job ${b.match(/^ {2}([\w-]+):/)[1]}`, body: code(b) }));
}

/** Violations in one job / composite action. */
export function violations({ name, body }) {
  const bad = [];
  const usesWasmPkg = /uses:\s*\.\/\.github\/actions\/engine-wasm-pkg\b/.test(body);
  const usesWasmPackCache = /uses:\s*\.\/\.github\/actions\/wasm-pack-cache\b/.test(body);
  if (/make\s+console-wasm|wasm-pack\s+build/.test(body) && !usesWasmPkg && !usesWasmPackCache) {
    bad.push(`${name}: runs wasm-pack without ./.github/actions/wasm-pack-cache (uncached binaryen/wasm-bindgen download)`);
  }
  if (/DUCKDB_DOWNLOAD_LIB/.test(body) && !/uses:\s*\.\/\.github\/actions\/libduckdb\b/.test(body)) {
    bad.push(`${name}: sets DUCKDB_DOWNLOAD_LIB without ./.github/actions/libduckdb (uncached libduckdb download)`);
  }
  return bad;
}

function scan() {
  const out = [];
  const wf = join(root, '.github', 'workflows');
  for (const f of readdirSync(wf)) out.push(...units(f, readFileSync(join(wf, f), 'utf8')));
  const actions = join(root, '.github', 'actions');
  // The two caching composites are the canonical providers; they may name what they cache.
  for (const d of readdirSync(actions).filter((d) => !['libduckdb', 'wasm-pack-cache'].includes(d))) {
    const p = join(actions, d, 'action.yml');
    if (existsSync(p)) out.push({ name: `actions/${d}`, body: code(readFileSync(p, 'utf8')) });
  }
  return out;
}

test('no workflow job or composite action does an uncached Releases download', () => {
  assert.deepEqual(scan().flatMap(violations), []);
});

test('the guard catches the defect class', () => {
  const wf = `on: push\njobs:\n  a:\n    steps:\n      - run: make console-wasm\n  b:\n    env:\n      DUCKDB_DOWNLOAD_LIB: '1'\n    steps:\n      - run: cargo test\n  c:\n    steps:\n      - uses: ./.github/actions/wasm-pack-cache\n      - run: make console-wasm\n`;
  assert.deepEqual(units('w.yml', wf).flatMap(violations), [
    'w.yml job a: runs wasm-pack without ./.github/actions/wasm-pack-cache (uncached binaryen/wasm-bindgen download)',
    'w.yml job b: sets DUCKDB_DOWNLOAD_LIB without ./.github/actions/libduckdb (uncached libduckdb download)',
  ]);
});

test('the engine-wasm-pkg composite installs wasm-pack through the cache', () => {
  const t = readFileSync(join(root, '.github', 'actions', 'engine-wasm-pkg', 'action.yml'), 'utf8');
  assert.match(code(t), /uses:\s*\.\/\.github\/actions\/wasm-pack-cache/);
});

test('libduckdb version derives from processos/Cargo.lock', () => {
  assert.equal(duckdbVersion('1.10504.0'), '1.5.4');
  assert.equal(duckdbVersion('1.10500.2'), '1.5.0');
  assert.equal(duckdbVersion('1.20103.0'), '2.1.3');
  const lock = readFileSync(join(root, 'processos', 'Cargo.lock'), 'utf8');
  assert.match(duckdbVersion(lockedCrateVersion(lock)), /^\d+\.\d+\.\d+$/);
});

// A job with system binaryen (wasm-opt on PATH) makes wasm-pack skip the
// binaryen download, so its tools-cache entry lacks binaryen. The key and the
// restore prefix must be partitioned on that, or a job without system binaryen
// restores the incomplete (immutable) entry and still hits Releases.
test('the wasm-pack tools cache is partitioned on system binaryen availability', () => {
  const t = code(readFileSync(join(root, '.github', 'actions', 'wasm-pack-cache', 'action.yml'), 'utf8'));
  assert.match(t, /command -v wasm-opt/);
  const keys = t.match(/wasm-pack-tools-[^\n]*/g) ?? [];
  assert.equal(keys.length, 2, 'expected a key and a restore-keys prefix');
  for (const k of keys) assert.match(k, /steps\.wasmpack\.outputs\.binaryen/, k);
});
