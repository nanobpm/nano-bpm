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
  // #1372: the `xwin` clippy-matrix entries make cargo-xwin download ~1.5 GB of
  // Microsoft CRT/SDK (aka.ms) per cold run. Any job that installs cargo-xwin
  // must restore it from the Actions cache, so an aka.ms outage cannot fail a
  // required gate on a warm cache.
  if (/tool:\s*cargo-xwin\b/.test(body) && !/uses:\s*\.\/\.github\/actions\/xwin-cache\b/.test(body)) {
    bad.push(`${name}: installs cargo-xwin without ./.github/actions/xwin-cache (uncached CRT/SDK download)`);
  }
  // #1684: the openapi-generator JAR came from a live Maven Central fetch per
  // run. Any job that runs codegen — directly or via a Makefile target whose
  // prerequisites regenerate generated/ — must restore the cached JAR.
  if (
    /make\s+(generate|all|all-release|build|release|release-gateway|debug|run|server-test|server-test-release|clippy)(?![\w-])|generate(-console)?\.sh/.test(body) &&
    !/uses:\s*\.\/\.github\/actions\/openapi-generator-cache\b/.test(body)
  ) {
    bad.push(`${name}: runs openapi-generator codegen without ./.github/actions/openapi-generator-cache (uncached Maven Central download)`);
  }
  return bad;
}

function scan() {
  const out = [];
  const wf = join(root, '.github', 'workflows');
  for (const f of readdirSync(wf)) out.push(...units(f, readFileSync(join(wf, f), 'utf8')));
  const actions = join(root, '.github', 'actions');
  // The caching composites are the canonical providers; they may name what they cache.
  for (const d of readdirSync(actions).filter((d) => !['libduckdb', 'wasm-pack-cache', 'openapi-generator-cache', 'xwin-cache'].includes(d))) {
    const p = join(actions, d, 'action.yml');
    if (existsSync(p)) out.push({ name: `actions/${d}`, body: code(readFileSync(p, 'utf8')) });
  }
  return out;
}

test('no workflow job or composite action does an uncached Releases download', () => {
  assert.deepEqual(scan().flatMap(violations), []);
});

test('the guard catches the defect class', () => {
  const wf = `on: push\njobs:\n  a:\n    steps:\n      - run: make console-wasm\n  b:\n    env:\n      DUCKDB_DOWNLOAD_LIB: '1'\n    steps:\n      - run: cargo test\n  c:\n    steps:\n      - uses: ./.github/actions/wasm-pack-cache\n      - run: make console-wasm\n  d:\n    steps:\n      - run: make generate\n  e:\n    steps:\n      - uses: ./.github/actions/openapi-generator-cache\n      - run: ./scripts/generate-console.sh\n  f:\n    steps:\n      - run: make generate-app-manifest\n  g:\n    steps:\n      - uses: taiki-e/install-action@v2\n        with:\n          tool: cargo-xwin\n  h:\n    steps:\n      - uses: taiki-e/install-action@v2\n        with:\n          tool: cargo-xwin\n      - uses: ./.github/actions/xwin-cache\n`;
  assert.deepEqual(units('w.yml', wf).flatMap(violations), [
    'w.yml job a: runs wasm-pack without ./.github/actions/wasm-pack-cache (uncached binaryen/wasm-bindgen download)',
    'w.yml job b: sets DUCKDB_DOWNLOAD_LIB without ./.github/actions/libduckdb (uncached libduckdb download)',
    'w.yml job d: runs openapi-generator codegen without ./.github/actions/openapi-generator-cache (uncached Maven Central download)',
    'w.yml job g: installs cargo-xwin without ./.github/actions/xwin-cache (uncached CRT/SDK download)',
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

test('openapi-generator cache key hashes the script that pins version + SHA-256 (#1684)', () => {
  const a = readFileSync(join(root, '.github', 'actions', 'openapi-generator-cache', 'action.yml'), 'utf8');
  assert.match(a, /hashFiles\('scripts\/lib-openapi-generator\.sh'\)/);
  const lib = readFileSync(join(root, 'scripts', 'lib-openapi-generator.sh'), 'utf8');
  assert.match(lib, /^OPENAPI_GENERATOR_DEFAULT_VERSION="[\d.]+"$/m);
  assert.match(lib, /^OPENAPI_GENERATOR_DEFAULT_SHA256="[0-9a-f]{64}"$/m);
  // Single source of truth: the generator scripts must not re-pin the version.
  for (const s of ['generate.sh', 'generate-console.sh']) {
    const body = readFileSync(join(root, 'scripts', s), 'utf8');
    assert.doesNotMatch(body, /OPENAPI_GENERATOR_VERSION=/, `${s} re-pins the generator version`);
  }
});

test('a cached openapi-generator JAR with a wrong SHA-256 fails loudly (#1684)', async () => {
  const { mkdtempSync, mkdirSync, writeFileSync, rmSync } = await import('node:fs');
  const { tmpdir } = await import('node:os');
  const { spawnSync } = await import('node:child_process');
  const dir = mkdtempSync(join(tmpdir(), 'oag-'));
  try {
    mkdirSync(join(dir, 'build', 'tools'), { recursive: true });
    const lib = join(root, 'scripts', 'lib-openapi-generator.sh');
    const run = () =>
      spawnSync('bash', ['-c', `set -euo pipefail; PROJECT_ROOT="$1"; source "$2"; ensure_openapi_generator_jar; echo "ok $JAR"`, '_', dir, lib], {
        encoding: 'utf8',
        // No network: a warm cache must never reach for a mirror.
        env: { ...process.env, OPENAPI_GENERATOR_VERSION: '0.0.0-test', OPENAPI_GENERATOR_SHA256: '9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08', https_proxy: 'http://127.0.0.1:9', HTTPS_PROXY: 'http://127.0.0.1:9' },
      });
    const jar = join(dir, 'build', 'tools', 'openapi-generator-cli-0.0.0-test.jar');
    writeFileSync(jar, 'test'); // sha256("test") matches the pin above
    let r = run();
    assert.equal(r.status, 0, r.stderr);
    assert.match(r.stdout, /^ok /);
    writeFileSync(jar, 'corrupt');
    r = run();
    assert.notEqual(r.status, 0);
    assert.match(r.stderr, /SHA-256 mismatch/);
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
});

test('overriding the generator version without a SHA-256 is rejected (#1684)', async () => {
  const { spawnSync } = await import('node:child_process');
  const lib = join(root, 'scripts', 'lib-openapi-generator.sh');
  const env = { ...process.env, OPENAPI_GENERATOR_VERSION: '0.0.1' };
  delete env.OPENAPI_GENERATOR_SHA256;
  const r = spawnSync('bash', ['-c', 'source "$1"', '_', lib], { encoding: 'utf8', env });
  assert.notEqual(r.status, 0);
  assert.match(r.stderr, /set OPENAPI_GENERATOR_SHA256/);
});
