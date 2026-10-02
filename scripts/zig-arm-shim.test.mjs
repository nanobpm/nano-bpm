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
 * Offending `cargo zigbuild` invocations in ONE shell's worth of script: ones
 * with no preceding source of the shim env script. Comment lines are ignored.
 * `lineOffset` maps block-relative lines back to the file.
 */
export function unshimmedZigbuilds(text, lineOffset = 0) {
  const lines = text.split('\n');
  const bad = [];
  let sourced = false;
  const sourceRe = new RegExp(`^\\s*(\\.|source)\\s+\\S*${SHIM_ENV.replace(/\./g, '\\.')}`);
  lines.forEach((line, i) => {
    const code = line.replace(/^\s*#.*$/, '');
    if (sourceRe.test(code)) sourced = true;
    if (/\bcargo zigbuild\b/.test(code) && !sourced) bad.push(`line ${i + 1 + lineOffset}`);
  });
  return bad;
}

/**
 * Split a file into the units that each run in their OWN shell, because an
 * exported variable does not survive past its shell (#1323 review):
 *  - workflows: every `run:` block (one-line or `|`/`>` block scalar);
 *  - Makefile: every recipe line (joined over `\` continuations);
 *  - shell scripts: the whole file.
 * Returns `{ text, offset }` pairs.
 */
export function shellUnits(path, text) {
  const lines = text.split('\n');
  if (/\.ya?ml$/.test(path)) {
    const units = [];
    for (let i = 0; i < lines.length; i++) {
      const m = lines[i].match(/^(\s*)(?:-\s+)?run:\s*(.*)$/);
      if (!m) continue;
      const keyIndent = lines[i].indexOf('run:');
      if (!/^[|>]/.test(m[2])) {
        units.push({ text: m[2], offset: i });
        continue;
      }
      const body = [];
      let j = i + 1;
      for (; j < lines.length; j++) {
        if (lines[j].trim() !== '' && lines[j].search(/\S/) <= keyIndent) break;
        body.push(lines[j]);
      }
      units.push({ text: body.join('\n'), offset: i + 1 });
      i = j - 1;
    }
    return units;
  }
  if (/(^|\/)Makefile$/.test(path)) {
    const units = [];
    for (let i = 0; i < lines.length; i++) {
      if (!lines[i].startsWith('\t')) continue;
      const start = i;
      const body = [lines[i]];
      while (lines[i].endsWith('\\') && i + 1 < lines.length) body.push(lines[++i]);
      units.push({ text: body.join('\n'), offset: start });
    }
    return units;
  }
  return [{ text, offset: 0 }];
}

export function offendersIn(path, text) {
  return shellUnits(path, text).flatMap((u) => unshimmedZigbuilds(u.text, u.offset));
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
    offendersIn(f, readFileSync(join(root, f), 'utf8')).map((l) => `${f} ${l}`),
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

test('sourcing in one shell does not cover zigbuild in another (#1323 review)', () => {
  const twoSteps = [
    '    steps:',
    '      - name: env',
    '        run: |',
    '          . ../scripts/zig-arm-shim-env.sh',
    '      - name: build',
    '        run: |',
    '          cargo zigbuild --release',
  ].join('\n');
  assert.deepEqual(offendersIn('.github/workflows/x.yml', twoSteps), ['line 7']);
  const oneStep = [
    '      - name: build',
    '        run: |',
    '          . ../scripts/zig-arm-shim-env.sh',
    '          cargo zigbuild --release',
  ].join('\n');
  assert.deepEqual(offendersIn('.github/workflows/x.yml', oneStep), []);
  assert.deepEqual(offendersIn('.github/workflows/x.yml', '      - run: cargo zigbuild'), ['line 1']);
  // Makefile: each recipe line is its own shell; continuations stay together.
  const mk = 'a:\n\t. scripts/zig-arm-shim-env.sh\n\tcargo zigbuild\nb:\n\t. scripts/zig-arm-shim-env.sh; \\\n\t  cargo zigbuild';
  assert.deepEqual(offendersIn('Makefile', mk), ['line 3']);
});
