// Proves the `engine-core` FFI surface works on `wasm32-unknown-unknown`.
//
// It loads the cdylib built by
//   cargo build --release --features ffi --target wasm32-unknown-unknown
// then (1) asserts every `nbpmn_*` C-ABI function is exported and (2)
// instantiates the module with no imports and drives a real deploy -> create
// -> complete cycle entirely through those exports, the same way a browser /
// JS host would. Exits non-zero (failing `make engine-wasm-ffi`) on any
// mismatch. Run via `make engine-wasm-ffi`.

import { readFileSync } from 'node:fs';
import { fileURLToPath } from 'node:url';
import { dirname, join } from 'node:path';

import { resolveTargetDir } from './lib-cargo-target-dir.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const engineCoreDir = join(here, '..');
// Resolve Cargo's EFFECTIVE target dir (#1716): the Makefile redirects
// CARGO_TARGET_DIR to a space-free path when the checkout path contains a
// space, so the cdylib is not necessarily under engine-core/target.
const wasmPath = join(
  resolveTargetDir(engineCoreDir),
  'wasm32-unknown-unknown',
  'release',
  'nanobpmn_engine_core.wasm',
);

const bytes = readFileSync(wasmPath);
const module = new WebAssembly.Module(bytes);

// 1) Every coarse FFI entry point (and linear memory) must be exported.
const exported = new Set(WebAssembly.Module.exports(module).map((e) => e.name));
const required = [
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
const missing = required.filter((name) => !exported.has(name));
if (missing.length > 0) {
  console.error('FAIL: missing wasm exports:', missing.join(', '));
  process.exit(1);
}
console.log('exports present:', required.join(', '));

// 2) Instantiate (no imports needed — the engine is std-only and self-contained)
// and run a process end to end through the boundary.
const { instance } = await WebAssembly.instantiate(bytes, {});
const x = instance.exports;
const view = () => new Uint8Array(x.memory.buffer);

// Copy a JS string into wasm linear memory; returns [ptr, len] to free later.
function writeStr(s) {
  const enc = new TextEncoder().encode(s);
  const ptr = x.nbpmn_alloc(enc.length);
  view().set(enc, ptr);
  return [ptr, enc.length];
}

function expect(condition, message) {
  if (!condition) {
    console.error('FAIL:', message);
    process.exit(1);
  }
}

const xml = `<?xml version="1.0" encoding="UTF-8"?>
<bpmn:definitions xmlns:bpmn="http://www.omg.org/spec/BPMN/20100524/MODEL">
  <bpmn:process id="p" isExecutable="true">
    <bpmn:startEvent id="s" />
    <bpmn:endEvent id="e" />
    <bpmn:sequenceFlow id="f" sourceRef="s" targetRef="e" />
  </bpmn:process>
</bpmn:definitions>`;

const engine = x.nbpmn_engine_new();
expect(engine !== 0, 'engine handle is null');

const [xmlPtr, xmlLen] = writeStr(xml);
const deployed = x.nbpmn_deploy_bpmn(engine, xmlPtr, xmlLen);
x.nbpmn_free(xmlPtr, xmlLen);
expect(BigInt(deployed) === 1n, `deploy returned ${deployed}, expected 1`);

const [idPtr, idLen] = writeStr('p');
const instanceKey = x.nbpmn_create_instance(engine, idPtr, idLen, 0n);
x.nbpmn_free(idPtr, idLen);
expect(BigInt(instanceKey) !== 0n, 'create_instance returned the 0 sentinel');

expect(
  x.nbpmn_is_completed(engine, instanceKey) === 1,
  'instance did not complete (start -> end should finish immediately)',
);
expect(BigInt(x.nbpmn_instance_count(engine)) === 1n, 'instance count != 1');

x.nbpmn_engine_free(engine);
console.log(`OK: deployed=1, instanceKey=${instanceKey}, completed=1 — FFI wasm32 round-trip succeeded`);
