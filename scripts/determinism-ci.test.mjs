// Guard (#2004): a failing producer inside the determinism `cell` must fail
// the cell. Command substitutions (and `||` contexts) suppress errexit, which
// once masked failed emit/replay steps behind a successful digest printf.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { spawnSync } from 'node:child_process';
import { mkdtempSync, writeFileSync, chmodSync, rmSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { dirname, join } from 'node:path';
import { fileURLToPath } from 'node:url';

const script = join(dirname(fileURLToPath(import.meta.url)), 'determinism-ci.sh');

// Stubs: corpus `emit` writes lines; replay writes a fixed record to --out.
function runCell({ emitFails = false, replayFails = false } = {}) {
  const dir = mkdtempSync(join(tmpdir(), 'det-ci-'));
  try {
    const bin = join(dir, 'bin');
    spawnSync('mkdir', ['-p', bin]);
    writeFileSync(join(bin, 'determinism_corpus'),
      `#!/usr/bin/env bash\n${emitFails ? 'exit 1' : 'echo x >"$2"; echo y >>"$2"; echo z >>"$2"'}\n`);
    writeFileSync(join(bin, 'determinism_replay'), `#!/usr/bin/env bash
${replayFails ? 'exit 1' : ''}
while [ $# -gt 0 ]; do case "$1" in --out) out="$2"; shift;; --snapshot-out) touch "$2"; shift;; --resume) resume=1;; esac; shift; done
# A resumed tail is empty so head + tail equals an uninterrupted run.
if [ -n "\${resume:-}" ]; then : >"$out"; else echo r >"$out"; fi
`);
    for (const f of ['determinism_corpus', 'determinism_replay']) chmodSync(join(bin, f), 0o755);
    return spawnSync('bash', [script, 'cell', join(dir, 'out')], {
      env: { ...process.env, DET_TEST_BIN: bin, DET_RUNS: '1' }, encoding: 'utf8',
    });
  } finally {
    rmSync(dir, { recursive: true, force: true });
  }
}

test('cell succeeds with healthy producers (control)', () => {
  const r = runCell(); assert.equal(r.status, 0, r.stderr + r.stdout);
});

test('cell fails when corpus emit fails', () => {
  assert.notEqual(runCell({ emitFails: true }).status, 0);
});

test('cell fails when replay fails', () => {
  assert.notEqual(runCell({ replayFails: true }).status, 0);
});
