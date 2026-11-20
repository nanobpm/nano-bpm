// Guards for the per-Zeebe-test-class unparseable ratchet (#1525).
// Run with `node --test formal/harvest/*.test.mjs`.

import assert from 'node:assert/strict';
import { mkdirSync, mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import os from 'node:os';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { buildReport } from './report.mjs';
import { BASELINE_FILE, checkRatchet, unparseableByClass } from './unparseable-ratchet.mjs';

const here = dirname(fileURLToPath(import.meta.url));
const A = 'zeebe/x/ATest';
const B = 'zeebe/x/BTest';

test('the committed unparseable list agrees with the committed per-class baseline', () => {
  const current = unparseableByClass(join(here, 'corpus'));
  const baseline = JSON.parse(readFileSync(BASELINE_FILE, 'utf8'));
  assert.deepEqual(checkRatchet(current, baseline), []);
});

test('a class newly unparseable fails the ratchet', () => {
  const errs = checkRatchet({ [A]: 1, [B]: 2 }, { classes: { [A]: 1 } });
  assert.equal(errs.length, 1);
  assert.match(errs[0], /BTest is newly unparseable/);
});

test('a class whose unparseable count grows fails the ratchet', () => {
  const errs = checkRatchet({ [A]: 3 }, { classes: { [A]: 1 } });
  assert.equal(errs.length, 1);
  assert.match(errs[0], /grew 1 -> 3/);
});

test('shrinkage must be locked into the baseline (shrink-only, no slack)', () => {
  assert.match(checkRatchet({ [A]: 1 }, { classes: { [A]: 2 } })[0], /shrank 2 -> 1/);
  assert.match(checkRatchet({}, { classes: { [A]: 2 } })[0], /shrank 2 -> 0/);
});

test('an unsorted or malformed baseline is rejected', () => {
  assert.match(checkRatchet({}, { classes: { [B]: 1, [A]: 1 } }).join('\n'), /sorted/);
  assert.equal(checkRatchet({}, {}).length, 1);
  assert.equal(checkRatchet({}, null).length, 1);
});

test('the report surfaces unparseable counts per test class, flagging fully unparseable families', () => {
  const dir = mkdtempSync(join(os.tmpdir(), 'harvest-ratchet-'));
  writeFileSync(join(dir, 'unparseable.json'), JSON.stringify([
    { id: 'a1', source: `${A}.java` }, { id: 'b1', source: `${B}.java` }, { id: 'b2', source: `${B}.java` },
  ]));
  mkdirSync(join(dir, 'ir'));
  writeFileSync(join(dir, 'ir', 'a0.json'), JSON.stringify({ id: 'a0', source: `${A}.java` }));
  const report = buildReport(dir);
  assert.deepEqual(report.unparseableByClass, [
    { testClass: A, unparseable: 1, harvested: 1, fullyUnparseable: false },
    { testClass: B, unparseable: 2, harvested: 0, fullyUnparseable: true },
  ]);
});
