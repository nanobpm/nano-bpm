// Emits the release, `wasm-opt`-minified FFI cdylib into `dist/engine-wasm-ffi/`
// alongside a JSON manifest (exports, imports, sha256, sizes, ABI + engine
// versions). This is the artifact downstream packages consume:
//
//   * @nanobpm/nano-engine-ffi (npm)         — jwulf/nano-bpm#10 deliverable 2
//   * io.github.jwulf:nano-engine-embedded    — jwulf/nano-bpm#10 deliverable 3
//
// It runs AFTER `make engine-wasm-ffi` (which already builds + verifies the
// unoptimized wasm), so we can assume the built cdylib exists and is functional.
// This script:
//   1. Runs `wasm-opt -Oz` on the built cdylib (fails cleanly if missing).
//   2. Copies the optimized wasm to `dist/engine-wasm-ffi/nano_engine.wasm`
//      (the stable published name — decoupled from the crate name).
//   3. Extracts exports + imports from the module (imports MUST be empty; the
//      engine is std-only and self-contained).
//   4. Computes sha256 + size and writes `manifest.json`.
//
// ABI_VERSION is bumped BY HAND in this file whenever `engine-core/src/ffi.rs`
// changes the C-ABI surface (new/removed export, changed signature/semantics).
// Downstream Java/JS packages should assert against this version at load time.

import { createHash } from 'node:crypto';
import { execFileSync } from 'node:child_process';
import { mkdirSync, readFileSync, statSync, writeFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

import { resolveTargetDir } from './lib-cargo-target-dir.mjs';

const ABI_VERSION = 2;

const here = dirname(fileURLToPath(import.meta.url));
const engineCoreDir = join(here, '..');
const projectRoot = join(engineCoreDir, '..');

const cargoToml = readFileSync(join(engineCoreDir, 'Cargo.toml'), 'utf8');
const engineVersionMatch = cargoToml.match(/^\s*version\s*=\s*"([^"]+)"/m);
if (!engineVersionMatch) die('could not read engine-core version from Cargo.toml');
const engineVersion = engineVersionMatch[1];

// Resolve Cargo's EFFECTIVE target dir (#1716): the Makefile redirects
// CARGO_TARGET_DIR to a space-free path when the checkout path contains a
// space, so the cdylib is not necessarily under engine-core/target — reading
// the default would fail on a fresh checkout or package a stale artifact.
const rawWasm = join(
  resolveTargetDir(engineCoreDir),
  'wasm32-unknown-unknown',
  'release',
  'nanobpmn_engine_core.wasm',
);
try {
  statSync(rawWasm);
} catch {
  die(`built wasm not found at ${rawWasm} — run \`make engine-wasm-ffi\` first`);
}

const distDir = join(projectRoot, 'dist', 'engine-wasm-ffi');
mkdirSync(distDir, { recursive: true });
const optWasm = join(distDir, 'nano_engine.wasm');

// 1) wasm-opt -Oz. Required — this is the release artifact; unoptimized is
// ~2× larger. Fail loudly rather than silently ship a bloated wasm.
try {
  execFileSync('wasm-opt', ['--version'], { stdio: 'ignore' });
} catch {
  die(
    'wasm-opt not found on PATH. Install Binaryen (macOS: `brew install binaryen`; ' +
      'Ubuntu: `sudo apt install binaryen`; other: https://github.com/WebAssembly/binaryen/releases).',
  );
}
console.log('wasm-opt -Oz ...');
execFileSync(
  'wasm-opt',
  [
    '-Oz',
    '--enable-bulk-memory',
    '--enable-nontrapping-float-to-int',
    '--enable-sign-ext',
    '--enable-mutable-globals',
    '--enable-multivalue',
    '--enable-reference-types',
    rawWasm,
    '-o',
    optWasm,
  ],
  { stdio: 'inherit' },
);

// 2) Inspect the optimized module: extract exports + imports and compute hash.
const bytes = readFileSync(optWasm);
const module = new WebAssembly.Module(bytes);
const exports = WebAssembly.Module.exports(module).map((e) => ({ name: e.name, kind: e.kind }));
const imports = WebAssembly.Module.imports(module).map((i) => ({
  module: i.module,
  name: i.name,
  kind: i.kind,
}));

if (imports.length > 0) {
  die(
    'wasm has unexpected imports (engine is std-only and must be self-contained): ' +
      JSON.stringify(imports),
  );
}

const REQUIRED_EXPORTS = [
  'memory',
  'nbpmn_alloc',
  'nbpmn_free',
  'nbpmn_engine_new',
  'nbpmn_engine_free',
  'nbpmn_deploy_bpmn',
  'nbpmn_create_instance',
  'nbpmn_correlate_message',
  'nbpmn_trigger_timers',
  'nbpmn_is_completed',
  'nbpmn_instance_count',
  // ABI v2: job worker surface
  'nbpmn_activate_jobs',
  'nbpmn_complete_job',
  'nbpmn_fail_job',
  'nbpmn_expire_jobs',
];
const exportNames = new Set(exports.map((e) => e.name));
const missing = REQUIRED_EXPORTS.filter((n) => !exportNames.has(n));
if (missing.length > 0) die(`wasm is missing required exports: ${missing.join(', ')}`);

const sha256 = createHash('sha256').update(bytes).digest('hex');
const rawSize = statSync(rawWasm).size;
const optSize = bytes.length;

// 3) Manifest. Consumer packages (npm + Maven) validate abi_version at load time.
const manifest = {
  artifact: 'nano_engine.wasm',
  abi_version: ABI_VERSION,
  engine_version: engineVersion,
  sha256,
  size_bytes: optSize,
  raw_size_bytes: rawSize,
  optimization: 'wasm-opt -Oz',
  target: 'wasm32-unknown-unknown',
  exports,
  imports,
  generated_at: new Date().toISOString(),
};
writeFileSync(join(distDir, 'manifest.json'), JSON.stringify(manifest, null, 2) + '\n');
writeFileSync(join(distDir, 'nano_engine.wasm.sha256'), `${sha256}  nano_engine.wasm\n`);

console.log(
  `OK: dist/engine-wasm-ffi/nano_engine.wasm — ` +
    `${(optSize / 1024).toFixed(1)} KiB (raw ${(rawSize / 1024).toFixed(1)} KiB), ` +
    `abi v${ABI_VERSION}, engine ${engineVersion}, sha256 ${sha256.slice(0, 12)}…`,
);

function die(msg) {
  console.error(`emit-dist: ${msg}`);
  process.exit(1);
}
