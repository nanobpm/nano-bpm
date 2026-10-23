// Guard (#1368): the Clippy -D warnings gate (which enforces the #1357
// unwrap_used/expect_used denies) must compile EVERY production code path.
// scripts/clippy-matrix.json is the single source of truth for the
// permutations; this test fails when a production crate, a declared cargo
// feature, or a #[cfg]/#[cfg_attr] predicate (feature x target_arch x target_os,
// including not(...)) is compiled by no matrix entry — or when ci.yml drifts
// from the matrix (raw `cargo clippy`, an entry whose job is not a required
// check, or a job that never invokes its entries).
import { execFileSync } from 'node:child_process';
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, symlinkSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';

import {
  ROOT,
  cfgPredicates,
  clippyArgs,
  coverageGaps,
  entryEnv,
  isMain,
  loadMatrix,
  reachable,
  requiredCheckNames,
} from './clippy-matrix.mjs';

const LIVE = loadMatrix();
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
      // Derived from the live matrix (no second copy of the target model to drift).
      hostCfg: LIVE.hostCfg,
      targets: LIVE.targets,
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

test('RED: unmodelled cfg keys fail closed (target_env, target_vendor, custom --cfg)', () => {
  const lib =
    '#[cfg(target_env = "msvc")]\nfn a() {}\n#[cfg(target_vendor = "apple")]\nfn b() {}\n#[cfg(nano_custom)]\nfn c() {}\n';
  const out = gaps(fixture({ cargo: '', lib, entries: [{}] })).join('\n');
  assert.match(out, /target_env = "msvc"/);
  assert.match(out, /target_vendor = "apple"/);
  assert.match(out, /cfg\(nano_custom\)/);
  // Genuinely free atoms stay reachable; modelled host keys evaluate.
  const ok = '#[cfg(debug_assertions)]\nfn d() {}\n#[cfg(target_env = "gnu")]\nfn e() {}\n#[cfg(target_pointer_width = "64")]\nfn f() {}\n';
  assert.deepEqual(gaps(fixture({ cargo: '', lib: ok, entries: [{}] })), []);
});

test('RED: cfg attributes not first on their line are still seen', () => {
  const lib = '#[inline] #[cfg(windows)] fn f() {}\nstruct S { #[cfg(target_os = "macos")] x: u8 }\n';
  const out = gaps(fixture({ cargo: '', lib, entries: [{}] })).join('\n');
  assert.match(out, /cfg\(windows\)/);
  assert.match(out, /target_os = "macos"/);
  const ignored = 'const S: &str = "#[cfg(windows)]"; /* #[cfg(windows)] */ // #[cfg(windows)]\n';
  assert.deepEqual(cfgPredicates(ignored), []);
});

test('RED: a char / byte-char literal holding a quote does not hide later cfgs', () => {
  for (const lit of [`'"'`, `b'"'`, `'\\''`, `b'\\"'`]) {
    const src = `const Q: char = ${lit};\n#[cfg(windows)]\nfn f<'a>(x: &'a str) {}\n`;
    assert.deepEqual(cfgPredicates(src), ['windows'], lit);
  }
  const lib = `const Q: char = '"';\n#[cfg(windows)]\nfn f() {}\n`;
  assert.match(gaps(fixture({ cargo: '', lib, entries: [{}] })).join('\n'), /cfg\(windows\)/);
});

test('RED: release-only and rustdoc-only code is not counted as linted', () => {
  const lib = '#[cfg(not(debug_assertions))]\nfn a() {}\n#[cfg(doc)]\nfn b() {}\n';
  const out = gaps(fixture({ cargo: '', lib, entries: [{}] })).join('\n');
  assert.match(out, /not\(debug_assertions\)/);
  assert.match(out, /cfg\(doc\)/);
});

test('RED: stacked cfgs and enclosing gated modules are evaluated together', () => {
  const cargo = '[features]\nrm = []\n';
  const entries = [{ features: ['rm'] }, { target: 'wasm32-unknown-unknown' }];
  const forms = {
    stacked: '#[cfg(feature = "rm")]\n#[cfg(target_arch = "wasm32")]\nfn f() {}\n',
    inlineMod: '#[cfg(feature = "rm")]\nmod m {\n    #[cfg(target_arch = "wasm32")]\n    fn f(a: u8, b: [u8; 2]) {}\n}\n',
    innerAttr: 'mod m {\n    #![cfg(feature = "rm")]\n    #[cfg(target_arch = "wasm32")]\n    fn f() {}\n}\n',
    genericImpl: '#[cfg(feature = "rm")]\nimpl<A, B> T for S<A, B> {\n    #[cfg(target_arch = "wasm32")]\n    fn f() {}\n}\n',
    genericFn: '#[cfg(feature = "rm")]\nfn g<A, B>() -> Vec<(A, B)> {\n    #[cfg(target_arch = "wasm32")]\n    let x = 1;\n}\n',
    whereClause: 'impl S {\n    #[cfg(feature = "rm")]\n    fn g<A>() where A: X, B: Y {\n        #[cfg(target_arch = "wasm32")]\n        let x = 1;\n    }\n}\n',
    fnBody: '#[cfg(feature = "rm")]\nfn g(a: u8, b: u8) {\n    #[cfg(target_arch = "wasm32")]\n    let x = 1;\n}\n',
  };
  for (const [name, lib] of Object.entries(forms)) {
    assert.match(gaps(fixture({ cargo, lib, entries })).join('\n'), /target_arch = "wasm32"/, name);
  }
  // External `#[cfg(..)] mod child;` gates the child file.
  const root = fixture({ cargo, lib: '#[cfg(feature = "rm")]\nmod child;\n', entries });
  writeFileSync(join(root, 'demo/src/child.rs'), '#[cfg(target_arch = "wasm32")]\npub fn f() {}\n');
  assert.match(gaps(root).join('\n'), /child\.rs: #\[cfg\(target_arch = "wasm32"\)\]/);
  // An UNGATED intermediate `mod grand;` keeps propagating the inherited gate.
  const deep = fixture({ cargo, lib: '#[cfg(feature = "rm")]\nmod child;\n', entries });
  writeFileSync(join(deep, 'demo/src/child.rs'), 'mod grand;\n');
  mkdirSync(join(deep, 'demo/src/child'), { recursive: true });
  writeFileSync(join(deep, 'demo/src/child/grand.rs'), '#[cfg(target_arch = "wasm32")]\npub fn f() {}\n');
  assert.match(gaps(deep).join('\n'), /grand\.rs: #\[cfg\(target_arch = "wasm32"\)\]/);
  // Siblings that end before the gated item do not leak into it.
  const ok = '#[cfg(feature = "rm")]\nfn a(x: u8) {}\n#[cfg(target_arch = "wasm32")]\nfn b() {}\n';
  assert.deepEqual(gaps(fixture({ cargo, lib: ok, entries })), []);
  // A generic field's comma does not end the field; the next field's own gate still applies alone.
  const okFields = 'struct S {\n    #[cfg(feature = "rm")]\n    a: Foo<A, B>,\n    #[cfg(target_arch = "wasm32")]\n    b: u8,\n}\n';
  assert.deepEqual(gaps(fixture({ cargo, lib: okFields, entries })), []);
});

test('RED: inline-module namespaces and #[path] modules keep their inherited gate', () => {
  const cargo = '[features]\nrm = []\n';
  const entries = [{ features: ['rm'] }, { target: 'wasm32-unknown-unknown' }];
  const child = '#[cfg(target_arch = "wasm32")]\npub fn f() {}\n';
  const inline = fixture({ cargo, lib: '#[cfg(feature = "rm")]\nmod outer {\n    mod child;\n}\n', entries });
  mkdirSync(join(inline, 'demo/src/outer'), { recursive: true });
  writeFileSync(join(inline, 'demo/src/outer/child.rs'), child);
  assert.match(gaps(inline).join('\n'), /outer\/child\.rs: #\[cfg\(target_arch = "wasm32"\)\]/);
  const pathed = fixture({ cargo, lib: '#[cfg(feature = "rm")]\n#[path = "different.rs"]\nmod child;\n', entries });
  writeFileSync(join(pathed, 'demo/src/different.rs'), child);
  assert.match(gaps(pathed).join('\n'), /different\.rs: #\[cfg\(target_arch = "wasm32"\)\]/);
});

test('RED: a --lib entry cannot cover code owned by a binary target', () => {
  const root = fixture({
    cargo: '',
    lib: 'pub fn ok() {}\n',
    entries: [{}, { target: 'wasm32-unknown-unknown', scope: 'lib' }],
  });
  mkdirSync(join(root, 'demo/src/bin'), { recursive: true });
  writeFileSync(join(root, 'demo/src/bin/tool.rs'), '#[cfg(target_arch = "wasm32")]\nfn f() {}\nfn main() {}\n');
  assert.match(gaps(root).join('\n'), /bin\/tool\.rs: #\[cfg\(target_arch = "wasm32"\)\]/);
  // The same site in the lib tree IS covered by the wasm32 --lib entry.
  const libOk = fixture({ cargo: '', lib: '#[cfg(target_arch = "wasm32")]\nfn f() {}\n', entries: [{}, { target: 'wasm32-unknown-unknown', scope: 'lib' }] });
  assert.deepEqual(gaps(libOk), []);
});

test('RED: a symlinked launch still runs main', () => {
  const dir = mkdtempSync(join(tmpdir(), 'clippy-matrix-link-'));
  const link = join(dir, 'clippy-matrix.mjs');
  symlinkSync(join(ROOT, 'scripts', 'clippy-matrix.mjs'), link);
  assert.ok(isMain(link));
  const out = execFileSync(process.execPath, [link, 'list'], { encoding: 'utf8' });
  assert.equal(out.trim().split('\n').length, LIVE.entries.length);
  assert.ok(!isMain(join(dir, 'missing.mjs')));
});

test('RED: a cfg introduced by (nested) cfg_attr gates the item', () => {
  const cargo = '[features]\nrm = []\n';
  const entries = [{ features: ['rm'] }, { target: 'wasm32-unknown-unknown' }];
  const forms = {
    direct: '#[cfg(feature = "rm")]\n#[cfg_attr(feature = "rm", cfg(target_arch = "wasm32"))]\nfn f() {}\n',
    nested: '#[cfg(feature = "rm")]\n#[cfg_attr(feature = "rm", inline, cfg_attr(unix, cfg(target_arch = "wasm32")))]\nfn f() {}\n',
    inner: 'mod m {\n    #![cfg(feature = "rm")]\n    #![cfg_attr(feature = "rm", cfg(target_arch = "wasm32"))]\n    #[cfg(unix)]\n    fn f() {}\n}\n',
  };
  for (const [name, lib] of Object.entries(forms)) {
    assert.notDeepEqual(gaps(fixture({ cargo, lib, entries })), [], name);
  }
  // An attribute-only payload still gates nothing.
  const ok = '#[cfg(feature = "rm")]\n#[cfg_attr(feature = "rm", allow(dead_code, unused))]\nfn f() {}\n';
  assert.deepEqual(gaps(fixture({ cargo, lib: ok, entries })), []);
});

test('wasm-cc-env.sh values with spaces survive both the runner and a shell recipe', () => {
  const env = { ...process.env, CC_wasm32_unknown_unknown: 'sccache clang', AR_wasm32_unknown_unknown: '/opt/my llvm/ar"x' };
  const got = entryEnv({ wasmCc: true }, ROOT, env);
  assert.equal(got.CC_wasm32_unknown_unknown, 'sccache clang');
  assert.equal(got.AR_wasm32_unknown_unknown, '/opt/my llvm/ar"x');
  // The Makefile splices the default output into a recipe via $(shell …) —
  // exercise that exact path (WASM_READ_MODEL_ENV) through make itself.
  const script = join(ROOT, 'scripts', 'wasm-cc-env.sh');
  const mk = `E = $(shell "${script}")\nall:\n\t@$(E) sh -c 'printf "%s\\n" "$$CC_wasm32_unknown_unknown" "$$AR_wasm32_unknown_unknown"'\n`;
  const out = execFileSync('make', ['-s', '-f', '-'], { env, input: mk, encoding: 'utf8' });
  assert.equal(out, 'sccache clang\n/opt/my llvm/ar"x\n');
});

test('RED→GREEN: a dependency-forced feature is on in every run that links it', () => {
  const root = fixture({ cargo: '', lib: '', entries: [] });
  const put = (p, t) => {
    mkdirSync(dirname(join(root, p)), { recursive: true });
    writeFileSync(join(root, p), t);
  };
  put('demo/Cargo.toml', '[workspace]\nmembers = ["crates/*"]\n');
  put('demo/crates/a/Cargo.toml', `[package]\nname = "a"\n\n[dependencies]\nb = { path = "../b", features = ["x"] }\n\n${LINTS}`);
  put('demo/crates/a/src/lib.rs', '');
  put('demo/crates/b/Cargo.toml', `[package]\nname = "b"\n\n[features]\nx = []\n\n${LINTS}`);
  put('demo/crates/b/src/lib.rs', '#[cfg(not(feature = "x"))]\nfn off() {}\n');
  const withEntries = (entries) => {
    const m = JSON.parse(readFileSync(join(root, 'scripts/clippy-matrix.json'), 'utf8'));
    writeFileSync(join(root, 'scripts/clippy-matrix.json'), JSON.stringify({ ...m, entries }));
    return gaps(root).join('\n');
  };
  const ws = { id: 'ws', job: 'j', dir: 'demo', workspace: true, target: 'host', scope: 'all-targets', features: [] };
  assert.match(withEntries([ws]), /crates\/b\/src\/lib\.rs: #\[cfg\(not\(feature = "x"\)\)\]/);
  const isolated = { ...ws, id: 'b', dir: 'demo/crates/b', workspace: false };
  assert.equal(withEntries([ws, isolated]), '');
});

test('RED: an inactive target-specific dependency cannot satisfy feature coverage', () => {
  for (const tgt of ["'cfg(windows)'", 'x86_64-pc-windows-msvc']) {
    const root = fixture({ cargo: '', lib: '', entries: [] });
    const put = (p, t) => {
      mkdirSync(dirname(join(root, p)), { recursive: true });
      writeFileSync(join(root, p), t);
    };
    put('demo/Cargo.toml', '[workspace]\nmembers = ["crates/*"]\n');
    put('demo/crates/a/Cargo.toml', `[package]\nname = "a"\n\n[target.${tgt}.dependencies]\nb = { path = "../b", features = ["x"] }\n\n${LINTS}`);
    put('demo/crates/a/src/lib.rs', '');
    put('demo/crates/b/Cargo.toml', `[package]\nname = "b"\n\n[features]\nx = []\n\n${LINTS}`);
    put('demo/crates/b/src/lib.rs', '#[cfg(feature = "x")]\nfn on() {}\n');
    const m = JSON.parse(readFileSync(join(root, 'scripts/clippy-matrix.json'), 'utf8'));
    const ws = { id: 'ws', job: 'j', dir: 'demo', workspace: true, target: 'host', scope: 'all-targets', features: [] };
    writeFileSync(join(root, 'scripts/clippy-matrix.json'), JSON.stringify({ ...m, entries: [ws] }));
    const out = gaps(root).join('\n');
    assert.match(out, /feature "x" is enabled by no clippy-matrix entry/, tgt);
    assert.match(out, /crates\/b\/src\/lib\.rs: #\[cfg\(feature = "x"\)\]/, tgt);
  }
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
  assert.match(
    run(wf('      # - run: node scripts/clippy-matrix.mjs run --job j\n      - run: echo\n')),
    /does not run `node scripts\/clippy-matrix.mjs run --job j`/,
  );
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
