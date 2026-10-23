// Guard (#1368): the Clippy -D warnings gate (which enforces the #1357
// unwrap_used/expect_used denies) must compile EVERY production code path.
// scripts/clippy-matrix.json is the single source of truth for the
// permutations; this test fails when a production crate, a declared cargo
// feature, or a #[cfg]/#[cfg_attr] predicate (feature x target_arch x target_os,
// including not(...)) is compiled by no matrix entry — or when ci.yml drifts
// from the matrix (raw `cargo clippy`, an entry whose job is not a required
// check, or a job that never invokes its entries).
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import {
  ROOT,
  cfgPredicates,
  clippyArgs,
  coverageGaps,
  loadMatrix,
  reachable,
  requiredCheckNames,
} from './clippy-matrix.mjs';

const workflow = readFileSync(join(ROOT, '.github', 'workflows', 'ci.yml'), 'utf8');

test('the live matrix covers every production crate, feature and cfg, and ci.yml runs it', () => {
  assert.deepEqual(coverageGaps({ workflow, requiredJobNames: requiredCheckNames() }), []);
});

test('every entry denies warnings and is runnable', () => {
  for (const e of loadMatrix().entries) {
    const args = clippyArgs(e);
    assert.deepEqual(args.slice(-3), ['--', '-D', 'warnings'], e.id);
    assert.ok(e.why, `entry ${e.id} documents why it exists`);
  }
});

// --- fixture repo: proves the guard goes RED on each uncovered surface -----

const LINTS = '[lints.clippy]\nunwrap_used = "deny"\nexpect_used = "deny"\n';

function fixture({ cargo, lib, entries, extra = {} }) {
  const root = mkdtempSync(join(tmpdir(), 'clippy-matrix-'));
  const put = (p, text) => {
    mkdirSync(dirname(join(root, p)), { recursive: true });
    writeFileSync(join(root, p), text);
  };
  put('demo/Cargo.toml', `[package]\nname = "demo"\n\n${cargo}\n${LINTS}`);
  put('demo/src/lib.rs', lib);
  put(
    'scripts/clippy-matrix.json',
    JSON.stringify({
      hostCfg: { target_arch: 'x86_64', target_os: 'linux', target_family: 'unix', unix: true, windows: false },
      targets: {
        'wasm32-unknown-unknown': { target_arch: 'wasm32', target_os: 'unknown', target_family: 'wasm', unix: false, windows: false },
      },
      entries: entries.map((e, i) => ({ id: `e${i}`, job: 'j', dir: 'demo', target: 'host', scope: 'all-targets', features: [], ...e })),
      excludedCrates: {},
      ...extra,
    }),
  );
  return root;
}

const gaps = (root) => coverageGaps({ root });

test('RED: a crate no entry lints', () => {
  const root = fixture({ cargo: '', lib: '', entries: [] });
  writeFileSync(join(root, 'scripts/clippy-matrix.json'), JSON.stringify({ ...loadMatrix(root), entries: [] }));
  assert.match(gaps(root).join('\n'), /crate demo is linted by no clippy-matrix entry/);
});

test('RED→GREEN: a declared feature no entry enables', () => {
  const cargo = '[features]\nextra = []\n';
  assert.match(gaps(fixture({ cargo, lib: '', entries: [{}] })).join('\n'), /feature "extra" is enabled by no/);
  assert.deepEqual(gaps(fixture({ cargo, lib: '', entries: [{ features: ['extra'] }] })), []);
});

test('RED→GREEN: not(feature) code when every entry enables the feature (default)', () => {
  const cargo = '[features]\ndefault = ["native"]\nnative = []\n';
  const lib = '#[cfg(not(feature = "native"))]\nfn fallback() {}\n';
  assert.match(gaps(fixture({ cargo, lib, entries: [{}] })).join('\n'), /not\(feature = "native"\)/);
  assert.deepEqual(gaps(fixture({ cargo, lib, entries: [{}, { noDefaultFeatures: true }] })), []);
});

test('RED→GREEN: a feature x target_arch combination (all(feature, wasm32))', () => {
  const cargo = '[features]\nrm = []\n';
  const lib = '#[cfg(all(feature = "rm", target_arch = "wasm32"))]\nfn w() {}\n';
  const host = { features: ['rm'] };
  const wasmOff = { target: 'wasm32-unknown-unknown' };
  assert.match(gaps(fixture({ cargo, lib, entries: [host, wasmOff] })).join('\n'), /target_arch = "wasm32"/);
  assert.deepEqual(gaps(fixture({ cargo, lib, entries: [host, { ...wasmOff, features: ['rm'] }] })), []);
});

test('RED: platform-only code (target_os / windows) the Linux runner never compiles', () => {
  const lib = '#[cfg(target_os = "macos")]\nfn m() {}\n#[cfg(windows)]\nfn w() {}\n';
  const out = gaps(fixture({ cargo: '', lib, entries: [{}] })).join('\n');
  assert.match(out, /target_os = "macos"/);
  assert.match(out, /cfg\(windows\)/);
});

test('unlintedCfg is an explicit, count-ratcheted allowlist (new sites and stale entries fail)', () => {
  const lib = '#[cfg(windows)]\nfn a() {}\n';
  const allow = (count) => ({ unlintedCfg: [{ file: 'demo/src/lib.rs', pred: 'windows', count, reason: 'r' }] });
  assert.deepEqual(gaps(fixture({ cargo: '', lib, entries: [{}], extra: allow(1) })), []);
  const two = `${lib}#[cfg(windows)]\nfn b() {}\n`;
  assert.match(gaps(fixture({ cargo: '', lib: two, entries: [{}], extra: allow(1) })).join('\n'), /occurs 2x/);
  assert.match(gaps(fixture({ cargo: '', lib: '', entries: [{}], extra: allow(1) })).join('\n'), /no longer exists/);
});

test('RED: a crate whose [lints.clippy] does not deny unwrap_used/expect_used', () => {
  const root = fixture({ cargo: '', lib: '', entries: [{}] });
  writeFileSync(join(root, 'demo/Cargo.toml'), '[package]\nname = "demo"\n');
  assert.match(gaps(root).join('\n'), /unwrap_used is not "deny"/);
});

test('RED: ci.yml drift — raw cargo clippy, unrequired job, job not invoking its entries', () => {
  const root = fixture({ cargo: '', lib: '', entries: [{}] });
  const wf = (body) => `jobs:\n  j:\n    name: J check\n    steps:\n${body}`;
  const required = new Set(['J check']);
  const run = (w, req = required) => coverageGaps({ root, workflow: w, requiredJobNames: req }).join('\n');
  assert.match(run(wf('      - run: cargo clippy -- -D warnings\n')), /runs `cargo clippy` directly/);
  assert.match(run(wf('      - run: echo\n')), /does not run `node scripts\/clippy-matrix.mjs run --job j`/);
  assert.match(run(wf('      - run: node scripts/clippy-matrix.mjs run --job j\n'), new Set()), /not a required check/);
  assert.equal(run(wf('      - run: node scripts/clippy-matrix.mjs run --job j\n')), '');
});

test('cfg predicate extraction + reachability', () => {
  const src = '#[cfg(feature = "a")]\n#[cfg_attr(not(feature = "b"), allow(dead_code))]\n/// #[cfg(doc_only)] in a comment\n';
  assert.deepEqual(cfgPredicates(src), ['feature = "a"', 'not(feature = "b")']);
  const conf = { features: new Set(['a']), cfg: { unix: true }, test: null };
  assert.equal(reachable('all(test, feature = "a")', conf), true);
  assert.equal(reachable('all(not(test), feature = "a")', { ...conf, test: false }), true);
  assert.equal(reachable('all(test, feature = "a")', { ...conf, test: false }), false);
  assert.equal(reachable('not(unix)', conf), false);
});
