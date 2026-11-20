// node --test formal/parity/coverage/report.test.mjs — the coverage report +
// ratchet logic (#1247), on synthetic llvm-cov / JaCoCo fixtures (no cargo,
// no JVM, no Docker).
import assert from 'node:assert/strict';
import { mkdtempSync, readFileSync, writeFileSync } from 'node:fs';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';
import { fileURLToPath } from 'node:url';
import { test } from 'node:test';

import {
  checkRatchet,
  floor1,
  parseJacoco,
  parseLlvmCov,
  tagNano,
  tagZeebe,
  updateBaseline,
} from './report.mjs';

const REPO = '/r';
const llvm = (extra = {}) => ({
  data: [
    {
      files: [{ filename: '/r/engine-core/src/a.rs', ...extra }],
      functions: [
        // two instantiations of one generic fn: the same regions, max count wins
        { filenames: ['/r/engine-core/src/a.rs'], regions: [[1, 1, 2, 1, 0, 0, 0, 0], [3, 1, 4, 1, 0, 0, 0, 0]] },
        { filenames: ['/r/engine-core/src/a.rs'], regions: [[1, 1, 2, 1, 5, 0, 0, 0], [3, 1, 4, 1, 0, 0, 0, 0]] },
        // a gap/skipped region (kind != 0) and a non-engine-core file are ignored
        { filenames: ['/r/engine-core/src/a.rs'], regions: [[9, 1, 9, 2, 0, 0, 0, 2]] },
        { filenames: ['/r/other/src/b.rs'], regions: [[1, 1, 1, 2, 0, 0, 0, 0]] },
      ],
    },
  ],
});

test('llvm-cov regions: de-duplicated across instantiations, engine-core only', () => {
  const m = parseLlvmCov(llvm(), { repo: REPO });
  assert.equal(m.unit, 'region');
  assert.equal(m.total, 2);
  assert.equal(m.covered, 1);
  assert.equal(m.percent, 50);
  assert.deepEqual(m.uncovered.map((s) => `${s.file}:${s.line}`), ['engine-core/src/a.rs:3']);
});

test('llvm-cov branches win over regions when the export carries them', () => {
  const m = parseLlvmCov(llvm({ branches: [[7, 3, 7, 9, 2, 0, 0, 0, 4]] }), { repo: REPO });
  assert.equal(m.unit, 'branch');
  assert.equal(m.total, 2);
  assert.equal(m.covered, 1);
  assert.deepEqual(m.uncovered.map((s) => [s.line, s.arm]), [[7, 'false']]);
});

test('nano triage: a registered extension, else a missing scenario', () => {
  const ext = [{ match: 'engine-core/src/ffi*', id: 'c-abi' }];
  const tagged = tagNano([{ file: 'engine-core/src/ffi.rs', line: 1 }, { file: 'engine-core/src/a.rs', line: 2 }], ext);
  assert.deepEqual(tagged.map((s) => s.tag), ['nano-extension:c-abi', 'missing-scenario']);
});

const JACOCO = `<?xml version="1.0"?><report name="x">
<package name="io/camunda/zeebe/engine/p"><class name="io/camunda/zeebe/engine/p/A"/>
<sourcefile name="A.java">
<line nr="10" mi="0" ci="3" mb="1" cb="1"/>
<line nr="11" mi="0" ci="3" mb="0" cb="2"/>
<line nr="12" mi="1" ci="0" mb="0" cb="0"/>
<line nr="20" mi="2" ci="0" mb="2" cb="0"/>
</sourcefile></package>
<package name="io/camunda/zeebe/engine/q"><sourcefile name="B.java"><line nr="5" mi="0" ci="1" mb="2" cb="0"/></sourcefile></package>
</report>`;

test('jacoco: branch counters per line; uncovered = any missed branch', () => {
  const m = parseJacoco(JACOCO);
  assert.equal(m.unit, 'branch');
  assert.equal(m.total, 8);
  assert.equal(m.covered, 3);
  assert.deepEqual(
    m.uncovered.map((s) => `${s.file}:${s.line}`),
    [
      'zeebe/engine/src/main/java/io/camunda/zeebe/engine/p/A.java:10',
      'zeebe/engine/src/main/java/io/camunda/zeebe/engine/p/A.java:20',
      'zeebe/engine/src/main/java/io/camunda/zeebe/engine/q/B.java:5',
    ],
  );
});

test('zeebe triage: tagged against the coverage matrix by line, then file, with status', () => {
  const surface = {
    cells: [
      { id: 'failure:A.m:X', sources: ['zeebe/engine/src/main/java/io/camunda/zeebe/engine/p/A.java:10'] },
      { id: 'rejection:A.n:Y', sources: ['zeebe/engine/src/main/java/io/camunda/zeebe/engine/p/A.java:99'] },
    ],
  };
  const rules = [{ match: 'failure:*', status: 'gap' }];
  const tagged = tagZeebe(parseJacoco(JACOCO).uncovered, surface, rules);
  assert.deepEqual(
    tagged.map((s) => [s.tag, s.scope, s.cells.map((c) => `${c.id}=${c.status}`)]),
    [
      ['missing-scenario', 'line', ['failure:A.m:X=gap']],
      ['missing-scenario', 'file', ['failure:A.m:X=gap', 'rejection:A.n:Y=unmapped']],
      ['missing-scenario', 'none', []],
    ],
  );
});

test('ratchet: a drop below the floor or a unit switch fails; at/above passes', () => {
  const base = { nano: { unit: 'region', floorPercent: 50.0 } };
  assert.deepEqual(checkRatchet({ nano: { unit: 'region', percent: 50, covered: 1, total: 2 } }, base), []);
  assert.match(checkRatchet({ nano: { unit: 'region', percent: 49.99, covered: 1, total: 2 } }, base)[0], /dropped below/);
  assert.match(checkRatchet({ nano: { unit: 'branch', percent: 90, covered: 9, total: 10 } }, base)[0], /re-baseline/);
  // an unmeasured side or an unseeded floor is not checked
  assert.deepEqual(checkRatchet({ zeebe: { unit: 'branch', percent: 1 } }, base), []);
});

test('ratchet: the documented slack absorbs live-broker scheduling variance (#1679)', () => {
  // zeebe JaCoCo measures against a live Camunda 8, so its branch count swings
  // run to run; `slackPercent` is the absorption band, applied at CHECK time
  // (effective floor = floorPercent − slackPercent), exactly once. The
  // observed 12.45% vs a 12.6% floor is inside the 0.2% band; 12.39% is
  // beyond it.
  const base = { zeebe: { unit: 'branch', floorPercent: 12.6, slackPercent: 0.2 } };
  assert.deepEqual(
    checkRatchet({ zeebe: { unit: 'branch', percent: 12.45, covered: 1033, total: 8296 } }, base),
    [],
    'a drop within the slack band passes',
  );
  assert.match(
    checkRatchet({ zeebe: { unit: 'branch', percent: 12.39, covered: 1028, total: 8296 } }, base)[0],
    /dropped below.*effective 12\.4%/,
    'a drop beyond the band fails and names the effective floor',
  );
  // The deterministic side (no slack) stays a hard floor.
  const nano = { nano: { unit: 'region', floorPercent: 15.9 } };
  assert.match(checkRatchet({ nano: { unit: 'region', percent: 15.89, covered: 1, total: 2 } }, nano)[0], /dropped below/);
});

test('update: floors only rise unless --allow-lower', () => {
  const base = { $comment: 'c', nano: { unit: 'region', floorPercent: 50.0 } };
  const up = updateBaseline({ nano: { unit: 'region', percent: 61.27 } }, base);
  assert.deepEqual(up.errors, []);
  assert.deepEqual(up.next, { $comment: 'c', nano: { unit: 'region', floorPercent: 61.2 } });
  assert.match(updateBaseline({ nano: { unit: 'region', percent: 40 } }, base).errors[0], /refusing/);
  assert.equal(updateBaseline({ nano: { unit: 'region', percent: 40 } }, base, { allowLower: true }).next.nano.floorPercent, 40);
  assert.equal(floor1(33.3399), 33.3);
  // a side's documented slack is kept but NOT subtracted from the floor it
  // seeds: checkRatchet applies it once, so seeding with it too would double it
  const slack = updateBaseline({ zeebe: { unit: 'branch', percent: 12.61 } }, { zeebe: { unit: 'branch', floorPercent: 12.0, slackPercent: 0.1 } });
  assert.deepEqual(slack.next.zeebe, { unit: 'branch', floorPercent: 12.6, slackPercent: 0.1 });
  // seed-then-check: the effective band below the measurement is exactly the slack
  assert.deepEqual(checkRatchet({ zeebe: { unit: 'branch', percent: 12.5 } }, slack.next), []);
  assert.match(checkRatchet({ zeebe: { unit: 'branch', percent: 12.49 } }, slack.next)[0], /dropped below/);
});

test('the committed baseline seeds a floor for both engines', () => {
  const b = JSON.parse(readFileSync(new URL('./baseline.json', import.meta.url), 'utf8'));
  for (const side of ['nano', 'zeebe']) {
    assert.ok(b[side], `baseline.json has no ${side} floor`);
    assert.equal(typeof b[side].floorPercent, 'number');
  }
  assert.equal(b.zeebe.unit, 'branch');
});

test('an empty measurement fails --check and --update instead of passing at 100%', () => {
  // pct() maps total === 0 to 100, so without the guard an empty report (a
  // failed collection, or a filter that matched nothing) would PASS the
  // ratchet and --update would raise the floor to 100% on nothing measured.
  const dir = mkdtempSync(join(tmpdir(), 'parity-cov-'));
  const write = (name, text) => {
    const p = join(dir, name);
    writeFileSync(p, text);
    return p;
  };
  const emptyNano = write('empty-nano.json', JSON.stringify({ data: [{ files: [], functions: [] }] }));
  const emptyJacoco = write('empty-jacoco.xml', '<?xml version="1.0"?><report name="x"></report>');
  const baseline = write('baseline.json', JSON.stringify({ nano: { unit: 'region', floorPercent: 50 }, zeebe: { unit: 'branch', floorPercent: 10 } }));
  const script = fileURLToPath(new URL('./report.mjs', import.meta.url));
  for (const args of [
    ['--nano', emptyNano, '--check'],
    ['--zeebe', emptyJacoco, '--check'],
    ['--nano', emptyNano, '--zeebe', emptyJacoco, '--update', '--allow-lower'],
  ]) {
    const r = spawnSync(process.execPath, [script, ...args, '--baseline', baseline], { encoding: 'utf8' });
    assert.notEqual(r.status, 0, `expected failure for ${args.join(' ')}: ${r.stdout}${r.stderr}`);
    assert.match(r.stderr, /no instrumented|measurement is empty/);
  }
  // --update must not have written a raised floor from the empty measurement.
  assert.deepEqual(JSON.parse(readFileSync(baseline, 'utf8')), { nano: { unit: 'region', floorPercent: 50 }, zeebe: { unit: 'branch', floorPercent: 10 } });
});
