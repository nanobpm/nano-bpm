// Guard (#668): `make engine-wasm-ffi-dist` emits the FFI wasm for the
// nano-bernd clients and must not depend on the browser package
// engine-wasm/pkg (wasm-pack). release-nano-bernd-{npm,jvm}.yml run it on a
// runner with no wasm-pack, so any `console-wasm` prerequisite, direct or
// transitive, would fail both client releases before dist is emitted.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { execFileSync } from 'node:child_process';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');

/** The recipe lines `make` would run for a target, without running them. */
export function dryRun(target) {
  return execFileSync('make', ['-n', '-C', root, target], { encoding: 'utf8' });
}

test('engine-wasm-ffi-dist never builds the wasm-pack browser package', () => {
  const plan = dryRun('engine-wasm-ffi-dist');
  assert.doesNotMatch(plan, /wasm-pack/, 'engine-wasm-ffi-dist must not reach console-wasm');
});

test('the browser-package probe target does build it (the detector sees wasm-pack)', () => {
  // Proves the detector above is live: this target depends on console-wasm.
  assert.match(dryRun('engine-wasm-agent-instance-e2e'), /wasm-pack/);
});
