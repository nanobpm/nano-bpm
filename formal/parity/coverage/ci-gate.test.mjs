// Guards the wiring of the nano parity-coverage ratchet (#1916). Main fell
// below the floor without anyone noticing: the job is not a required check,
// so the PR that caused the drop merged anyway, and every later main push
// skipped the job through its path filter, so main looked green while any PR
// that happened to trip the filter was blocked by an unrelated drop.
import { test } from 'node:test';
import assert from 'node:assert/strict';
import { readFileSync } from 'node:fs';

const read = (rel) => readFileSync(new URL(rel, import.meta.url), 'utf8');
const ci = read('../../../.github/workflows/ci.yml');
const makefile = read('../../../Makefile');

/** The text of one top-level job in ci.yml (up to the next job key). */
function job(name) {
  const start = ci.indexOf(`\n  ${name}:\n`);
  assert.ok(start >= 0, `ci.yml has no job ${name}`);
  const rest = ci.slice(start + 1);
  const next = rest.slice(1).search(/\n {2}[A-Za-z0-9_-]+:\n/);
  return next < 0 ? rest : rest.slice(0, next + 1);
}

test('parity-coverage-nano runs on every push to main, not only path-filtered ones', () => {
  const ifLine = job('parity-coverage-nano').match(/^ {4}if: (.*)$/m);
  assert.ok(ifLine, 'parity-coverage-nano has no if: condition');
  assert.match(ifLine[1], /github\.event_name == 'push'/);
});

test('the paritycoverage path filter covers engine-core and the corpus', () => {
  const filter = ci.match(/\n {12}paritycoverage:\n((?: {14}- .*\n)+)/);
  assert.ok(filter, 'ci.yml has no paritycoverage filter');
  for (const p of ['engine-core/**', 'formal/parity/runtime/corpus/**', 'formal/parity/coverage/**']) {
    assert.ok(filter[1].includes(`'${p}'`), `paritycoverage filter misses ${p}`);
  }
});

test('make parity-coverage-nano runs the same measure + ratchet as CI', () => {
  const body = job('parity-coverage-nano');
  const target = makefile.match(/\nparity-coverage-nano:.*\n((?:\t.*\n)+)/);
  assert.ok(target, 'Makefile has no parity-coverage-nano target');
  for (const cmd of ['formal/parity/coverage/nano-coverage.sh', 'formal/parity/coverage/report.mjs --nano']) {
    assert.ok(body.includes(cmd), `ci.yml job no longer runs ${cmd}`);
    assert.ok(target[1].includes(cmd), `make target does not run ${cmd}`);
  }
  assert.match(body, /report\.mjs .*--check/);
  assert.match(target[1], /report\.mjs .*--check/);
  // The bounded-exhaustive bound must match too, or local and CI measure
  // different corpora.
  assert.match(body, /nano-coverage\.sh "[^"]+" ci\n/);
  assert.match(target[1], /nano-coverage\.sh "[^"]+" ci\n/);
});
