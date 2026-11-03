// Pin-consistency and report-shape guards for the harvest pipeline (#1249).
// Run with `node --test formal/harvest/*.test.mjs`.

import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { join, dirname } from 'node:path';
import os from 'node:os';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';
import { buildReport } from './report.mjs';

const here = dirname(fileURLToPath(import.meta.url));

test('the harvest pin stays on the same Zeebe sha as the parity surface pin', () => {
  // The harvested scenarios tag against the coverage matrix, which is extracted
  // from formal/parity/zeebe-pin.json's sha. If the two drift apart, the tags
  // would describe a different Zeebe than the matrix measures.
  const harvestPin = JSON.parse(readFileSync(join(here, 'zeebe-test-pin.json'), 'utf8'));
  const parityPin = JSON.parse(readFileSync(join(here, '..', 'parity', 'zeebe-pin.json'), 'utf8'));
  assert.equal(harvestPin.sha, parityPin.sha,
    'zeebe-test-pin.json sha must equal formal/parity/zeebe-pin.json sha');
  assert.equal(harvestPin.repository, parityPin.repository);
});

test('every pinned model artifact carries a sha256 (supply-chain pin)', () => {
  const pin = JSON.parse(readFileSync(join(here, 'zeebe-test-pin.json'), 'utf8'));
  assert.ok(pin.modelArtifacts.length > 0);
  for (const a of pin.modelArtifacts) {
    assert.match(a.sha256, /^[0-9a-f]{64}$/, `${a.artifactId} must pin a sha256`);
    assert.ok(a.groupId && a.artifactId && a.version);
  }
});

test('buildReport aggregates the four acceptance counts', () => {
  // A synthetic corpus dir exercising the aggregation without the pipeline.
  const dir = mkdtempSync(join(os.tmpdir(), 'harvest-report-'));
  writeFileSync(join(dir, 'harvest-summary.json'), JSON.stringify({ scannedFiles: 10, harvested: 5 }));
  writeFileSync(join(dir, 'normalised.json'), JSON.stringify([{ id: 'a' }, { id: 'b' }]));
  writeFileSync(join(dir, 'out-of-scope.json'), JSON.stringify([{ id: 'c' }]));
  writeFileSync(join(dir, 'unparseable.json'), JSON.stringify([{ id: 'd' }, { id: 'e' }]));
  writeFileSync(join(dir, 'run-results.json'), JSON.stringify([
    { id: 'a', processId: 'p', status: 'runnable', detail: 'completed-cleanly' },
    { id: 'b', processId: 'p', status: 'unverified', detail: 'instance-did-not-complete' },
  ]));
  const report = buildReport(dir);
  assert.equal(report.counts.scannedFiles, 10);
  assert.equal(report.counts.harvested, 5);
  assert.equal(report.counts.runnable, 1);
  assert.equal(report.counts.unverified, 1);
  // out-of-scope = unparseable(2) + normalise(1) + run(0)
  assert.equal(report.counts.outOfScope, 3);
  assert.equal(report.unverified.length, 1);
  assert.equal(report.unverified[0].processId, 'p');
  // A non-clean Nano run is never labelled a parity divergence: the report has
  // no `diverging` field at all (#1397 review — no Zeebe oracle is harvested).
  assert.ok(!('diverging' in report));
  assert.ok(!('diverging' in report.counts));
});
