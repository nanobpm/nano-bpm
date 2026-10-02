// Guard (#1325): the compiler cache is an accelerator, never a correctness
// dependency. A GHA cache-backend outage ("ServerBusy: Egress is over the
// account limit") made sccache fail at server startup and, because every Rust
// job set RUSTC_WRAPPER=sccache unconditionally, failed the job before it
// compiled anything. CI must wire sccache ONLY through the fail-open composite
// action .github/actions/sccache, which enables RUSTC_WRAPPER only once the
// server has started.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, readdirSync, writeFileSync, chmodSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';
import { execFileSync } from 'node:child_process';

const root = join(dirname(fileURLToPath(import.meta.url)), '..');
const START = join(root, '.github', 'actions', 'sccache', 'start.sh');
const COMPOSITE = join('.github', 'actions', 'sccache', 'action.yml');

/**
 * Lines that wire sccache directly instead of through the composite action:
 * YAML `env:` mappings (`NAME: v`) and shell assignments (`NAME=v`, as in
 * `export NAME=v` or `echo "NAME=v" >> "$GITHUB_ENV"`), including
 * `CARGO_BUILD_RUSTC_WRAPPER`.
 */
export function directSccacheWiring(text) {
  return text
    .split('\n')
    .map((line, i) => ({ code: line.replace(/^\s*#.*$/, ''), n: i + 1 }))
    .filter(({ code }) => /(RUSTC_WRAPPER|SCCACHE_[A-Z_]+)\s*[:=]|mozilla-actions\/sccache-action/.test(code))
    .map(({ n }) => `line ${n}`);
}

test('no workflow wires sccache except via .github/actions/sccache', () => {
  const dir = join(root, '.github', 'workflows');
  const offenders = readdirSync(dir).flatMap((f) =>
    directSccacheWiring(readFileSync(join(dir, f), 'utf8')).map((l) => `${f} ${l}`),
  );
  assert.deepEqual(offenders, []);
  // The composite is the one place allowed to use the upstream action.
  assert.match(readFileSync(join(root, COMPOSITE), 'utf8'), /mozilla-actions\/sccache-action@/);
});

test('detector flags direct wiring and ignores comments', () => {
  assert.deepEqual(directSccacheWiring("    env:\n      RUSTC_WRAPPER: 'sccache'"), ['line 2']);
  assert.deepEqual(directSccacheWiring('      - uses: mozilla-actions/sccache-action@v0.0.10'), ['line 1']);
  assert.deepEqual(directSccacheWiring("      SCCACHE_GHA_ENABLED: 'true'"), ['line 1']);
  assert.deepEqual(directSccacheWiring('          echo "RUSTC_WRAPPER=sccache" >> "$GITHUB_ENV"'), ['line 1']);
  assert.deepEqual(directSccacheWiring('          export SCCACHE_GHA_ENABLED=true'), ['line 1']);
  assert.deepEqual(directSccacheWiring("      CARGO_BUILD_RUSTC_WRAPPER: sccache"), ['line 1']);
  assert.deepEqual(directSccacheWiring('    # RUSTC_WRAPPER: sccache\n      - uses: ./.github/actions/sccache'), []);
});

function runStart(fakeExit) {
  const dir = mkdtempSync(join(tmpdir(), 'sccache-'));
  const fake = join(dir, 'fake-sccache');
  writeFileSync(
    fake,
    `#!/bin/sh\necho "sccache: error: Server startup failed: cache storage failed to read: ServerBusy"\nexit ${fakeExit}\n`,
  );
  chmodSync(fake, 0o755);
  const envFile = join(dir, 'env');
  writeFileSync(envFile, '');
  const stdout = execFileSync('bash', [START], {
    env: { PATH: process.env.PATH, GITHUB_ENV: envFile, SCCACHE: fake },
  }).toString();
  return { env: readFileSync(envFile, 'utf8'), stdout, fake };
}

test('server startup failure degrades to plain rustc with a warning (exit 0)', () => {
  const { env, stdout } = runStart(2);
  assert.doesNotMatch(env, /RUSTC_WRAPPER/);
  assert.match(env, /^CARGO_INCREMENTAL=0$/m);
  assert.match(stdout, /^::warning title=sccache unavailable::.*ServerBusy/m);
});

test('server startup success enables the wrapper, fail-open for mid-build I/O errors', () => {
  const { env, fake } = runStart(0);
  assert.match(env, new RegExp(`^RUSTC_WRAPPER=${fake}$`, 'm'));
  assert.match(env, /^SCCACHE_GHA_ENABLED=true$/m);
  assert.match(env, /^SCCACHE_IGNORE_SERVER_IO_ERROR=1$/m);
  assert.match(env, /^CARGO_INCREMENTAL=0$/m);
});
