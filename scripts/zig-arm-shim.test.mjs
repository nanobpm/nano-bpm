// Guard (#1322): every `cargo zigbuild` call site must source
// scripts/zig-arm-shim-env.sh, the single source of truth for linking the
// ARM-RTABI unaligned-access shim into armv6 binaries. Since Rust 1.99 (LLVM 23)
// the strict-align armv6 libstd calls `__aeabi_uread4` & co.; cargo-zigbuild
// drops Rust's compiler_builtins on ARM and zig's compiler-rt lacks them, so a
// zigbuild path that skips the shim fails the armv6 link (v0.0.26).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const SHIM_ENV = 'zig-arm-shim-env.sh';
const SHIM_C = join(root, 'server', 'build-support', 'aeabi_unaligned.c');

/**
 * Offending `cargo zigbuild` invocations: ones with no preceding source of the
 * shim env script in the same file. Comment lines are ignored.
 */
export function unshimmedZigbuilds(text) {
  const lines = text.split('\n');
  const bad = [];
  let sourced = false;
  lines.forEach((line, i) => {
    const code = line.replace(/^\s*#.*$/, '');
    if (new RegExp(`^\\s*(\\.|source)\\s+\\S*${SHIM_ENV.replace(/\./g, '\\.')}`).test(code)) sourced = true;
    if (/\bcargo zigbuild\b/.test(code) && !sourced) bad.push(`line ${i + 1}`);
  });
  return bad;
}

function zigbuildCallSites() {
  const files = [
    ...readdirSync(join(root, '.github', 'workflows')).map((f) => join('.github', 'workflows', f)),
    ...readdirSync(join(root, 'scripts')).filter((f) => f.endsWith('.sh')).map((f) => join('scripts', f)),
    'Makefile',
  ];
  return files.filter((f) => /\bcargo zigbuild\b/.test(readFileSync(join(root, f), 'utf8')));
}

test('the shim source exists and defines all four weak helpers', () => {
  assert.ok(existsSync(SHIM_C), `${SHIM_C} missing`);
  const c = readFileSync(SHIM_C, 'utf8');
  for (const sym of ['__aeabi_uread4', '__aeabi_uwrite4', '__aeabi_uread8', '__aeabi_uwrite8']) {
    assert.match(c, new RegExp(`__attribute__\\(\\(weak\\)\\)[^\\n]*\\b${sym}\\(`), `${sym} must be weak`);
  }
});

test('every cargo zigbuild call site sources the shim env first', () => {
  const sites = zigbuildCallSites();
  assert.ok(sites.length >= 2, `expected the workflow + cross-build.sh, found ${sites.join(', ')}`);
  const offenders = sites.flatMap((f) =>
    unshimmedZigbuilds(readFileSync(join(root, f), 'utf8')).map((l) => `${f} ${l}`),
  );
  assert.deepEqual(offenders, []);
});

test('the env script exports an absolute link-arg to the shim for armv6 only', () => {
  const out = execFileSync(
    'bash',
    ['-c', `. "${join(root, 'scripts', SHIM_ENV)}"; printf '%s' "$CARGO_TARGET_ARM_UNKNOWN_LINUX_GNUEABIHF_RUSTFLAGS"`],
    { cwd: '/', env: { PATH: process.env.PATH } },
  ).toString();
  assert.equal(out, `-C link-arg=${SHIM_C}`);
  // Pre-existing flags are kept, not clobbered.
  const kept = execFileSync(
    'bash',
    ['-c', `. "${join(root, 'scripts', SHIM_ENV)}"; printf '%s' "$CARGO_TARGET_ARM_UNKNOWN_LINUX_GNUEABIHF_RUSTFLAGS"`],
    { cwd: '/', env: { PATH: process.env.PATH, CARGO_TARGET_ARM_UNKNOWN_LINUX_GNUEABIHF_RUSTFLAGS: '-C opt-level=s' } },
  ).toString();
  assert.equal(kept, `-C opt-level=s -C link-arg=${SHIM_C}`);
});

test('detector flags a zigbuild with no preceding shim source', () => {
  assert.deepEqual(unshimmedZigbuilds('cargo zigbuild --release'), ['line 1']);
  assert.deepEqual(unshimmedZigbuilds('. ../scripts/zig-arm-shim-env.sh\ncargo zigbuild --release'), []);
  assert.deepEqual(unshimmedZigbuilds('# . scripts/zig-arm-shim-env.sh\ncargo zigbuild'), ['line 2']);
  assert.deepEqual(unshimmedZigbuilds('cargo zigbuild\n. scripts/zig-arm-shim-env.sh'), ['line 1']);
});
