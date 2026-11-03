// Tests for the coverage guard (#1245): one test per failure mode it must catch.
import assert from 'node:assert/strict';
import { existsSync, readFileSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import { CORPUS, baselineGrowth, evaluate, globToRegExp, markdownReport, shrinkGaps, tally } from './check.mjs';
import { coarseCellId } from './extract.mjs';

const RS = 'engine-core/src/engine/tests/lifecycle.rs';
const FILES = {
  [RS]: '// zeebe-cells: element:Task\n#[test]\nfn runs_a_task() {}\n\n#[test]\n#[should_panic]\n// zeebe-cells: element:Gone\nfn stale() {}\n\n// zeebe-cells: element:Task\n#[test]\n#[ignore = \"slow\"]\nfn skipped() {}\n\n#[test]\nfn bare() {}\n\n// zeebe-cells: element:Task\nfn helper() {}\n',
  [`${CORPUS}reject-x.bpmn`]: '<!-- verdict: reject | category: X -->\n<!-- zeebe-cells: validation:V:m -->\n<definitions/>',
  [`${CORPUS}reject-z.bpmn`]: '<!-- verdict: reject | category: Z -->\n<definitions/>',
  [`${CORPUS}reject-stale.bpmn`]: '<!-- verdict: reject | category: Z -->\n<!-- zeebe-cells: validation:V:m validation:V:gone -->',
  [`${CORPUS}diverge-y.bpmn`]: '<!-- verdict: diverge | category: Y -->\n<definitions/>',
};
const fs = { exists: (p) => p in FILES, read: (p) => FILES[p] };
const PIN = { repository: 'r', ref: 'stable/x', sha: 'abc' };
const surface = (...ids) => ({ zeebe: { ...PIN }, cells: ids.map((id) => ({ id, sources: ['s'] })) });
// Unless a test is about the ratchet, the baseline is exactly the cells gap rules claim.
const gapsOf = (ids, rules) =>
  ids.filter((id) => rules.find((r) => typeof r?.match === 'string' && globToRegExp(r.match).test(id))?.status === 'gap').sort();
const check = (ids, rules, gaps = gapsOf(ids, rules)) => evaluate(surface(...ids), { reviewedAt: 'abc', rules }, fs, gaps, PIN).errors;

const tested = { status: 'nano-tested', evidence: [`${RS}::runs_a_task`] };
const gap = { status: 'gap', issue: 1249, note: 'later' };

test('a fully mapped surface passes', () => {
  assert.deepEqual(
    check(['element:Task', 'intent:Job:CREATED'], [{ match: 'element:Task', ...tested }, { match: 'intent:*', ...gap }]),
    [],
  );
});

test('an unmapped cell fails — including one a Zeebe bump adds', () => {
  assert.deepEqual(check(['element:Task', 'element:New'], [{ match: 'element:Task', ...tested }]), [
    'unmapped cell: element:New',
  ]);
});

test('a dead rule fails, and the first matching rule wins', () => {
  const errors = check(['element:Task'], [{ match: 'element:*', ...gap }, { match: 'element:Task', ...tested }]);
  assert.deepEqual(errors, ['dead rule (claims no cell): element:Task']);
  const { assignments } = evaluate(surface('element:Task'), { rules: [{ match: 'element:*', ...gap }] }, fs, ['element:Task']);
  assert.equal(assignments.get('element:Task'), 'gap');
});

test('nano-tested evidence must name an existing #[test] fn', () => {
  const rule = (ref) => [{ match: 'element:Task', status: 'nano-tested', evidence: [ref] }];
  assert.match(check(['element:Task'], rule(`${RS}::renamed_away`))[0], /no #\[test\] fn renamed_away/);
  assert.match(check(['element:Task'], rule(`${RS}::helper`))[0], /no #\[test\] fn helper/);
  assert.match(check(['element:Task'], rule(`${RS}::skipped`))[0], /fn skipped in .* is #\[ignore\]d, so CI never runs it/);
  assert.match(check(['element:Task'], rule('engine-core/gone.rs::runs_a_task'))[0], /missing file/);
  assert.match(check(['element:Task'], rule(RS))[0], /must be path::test_fn/);
  assert.match(check(['element:Task'], [{ match: 'element:Task', status: 'nano-tested' }])[0], /needs evidence/);
});

test('nano-tested tests must declare every cell the rule claims, and only real cells', () => {
  const rule = (fn, match = 'element:Task') => [{ match, status: 'nano-tested', evidence: [`${RS}::${fn}`] }];
  assert.deepEqual(check(['element:Task'], rule('runs_a_task')), []);
  assert.deepEqual(check(['element:Task', 'element:Other'], rule('runs_a_task', 'element:*')), [
    'rule "element:*": no evidence declares element:Other',
  ]);
  assert.deepEqual(check(['element:Task'], rule('bare')), [
    `rule "element:Task": ${RS}::bare declares no zeebe-cells`,
    'rule "element:Task": no evidence declares element:Task',
  ]);
  // The declaration is read from the test's own attribute block, past other attributes.
  assert.deepEqual(check(['element:Task'], rule('stale')), [
    `${RS}::stale declares a cell not in zeebe-surface.json: element:Gone`,
    'rule "element:Task": no evidence declares element:Task',
  ]);
});

test('parity evidence must be a corpus fixture with a Zeebe verdict', () => {
  const rule = (ref) => [{ match: 'validation:V:m', status: 'parity', evidence: [ref] }];
  assert.deepEqual(check(['validation:V:m'], rule(`${CORPUS}reject-x.bpmn`)), []);
  assert.match(check(['validation:V:m'], rule(`${CORPUS}diverge-y.bpmn`))[0], /'diverge' is not Zeebe parity/);
  assert.match(check(['validation:V:m'], rule(`${CORPUS}absent.bpmn`))[0], /missing fixture/);
  assert.match(check(['validation:V:m'], rule(`${RS}::runs_a_task`))[0], /must be a .*\.bpmn fixture/);
});

test('parity fixtures must declare every cell the rule claims, and only real cells', () => {
  const rule = (match, ref) => [{ match, status: 'parity', evidence: [ref] }];
  assert.deepEqual(check(['validation:V:m', 'validation:V:n'], rule('validation:V:*', `${CORPUS}reject-x.bpmn`)), [
    'rule "validation:V:*": no evidence declares validation:V:n',
  ]);
  assert.match(check(['validation:V:m'], rule('validation:V:m', `${CORPUS}reject-z.bpmn`))[0], /declares no zeebe-cells/);
  assert.deepEqual(check(['validation:V:m'], rule('validation:V:m', `${CORPUS}reject-stale.bpmn`)), [
    `${CORPUS}reject-stale.bpmn declares a cell not in zeebe-surface.json: validation:V:gone`,
  ]);
});

test('gap and out-of-scope need an issue and a note, and take no evidence', () => {
  const cell = ['intent:Scale:SCALE_UP'];
  assert.match(check(cell, [{ match: 'intent:*', status: 'gap', note: 'n' }])[0], /needs an issue/);
  assert.match(check(cell, [{ match: 'intent:*', status: 'out-of-scope', issue: 1240 }])[0], /needs a note/);
  assert.match(check(cell, [{ match: 'intent:*', ...gap, evidence: ['x'] }])[0], /takes no evidence/);
  assert.match(check(cell, [{ match: 'intent:*', ...gap, issue: '#1249' }])[0], /needs an issue number/);
});

test('malformed rules fail', () => {
  assert.match(check(['element:Task'], [{ match: 'element:Task', ...tested, why: 'x' }])[0], /unknown key 'why'/);
  assert.match(check(['element:Task'], [{ match: 'element:Task', status: 'done' }])[0], /status must be one of/);
  assert.deepEqual(evaluate(surface('element:Task'), {}, fs, []).errors, ['coverage.json must have a rules array', 'unmapped cell: element:Task']);
});

test('a surface extracted from another Zeebe repository, ref or revision fails', () => {
  for (const k of ['repository', 'ref', 'sha']) {
    const errors = evaluate(surface('element:Task'), { reviewedAt: 'abc', rules: [{ match: '*', ...gap }] }, fs, ['element:Task'], { ...PIN, [k]: 'other' }).errors;
    assert.match(errors[0], /regenerate it$/, k);
  }
});

test('a pin bump fails until coverage.json is reviewed at the new pin', () => {
  const rules = [{ match: 'intent:*', ...gap }];
  const bumped = { ...PIN, sha: 'new' };
  const s = { zeebe: bumped, cells: [{ id: 'intent:Job:CREATED', sources: ['s'] }] };
  const errors = evaluate(s, { reviewedAt: 'abc', rules }, fs, ['intent:Job:CREATED'], bumped).errors;
  assert.deepEqual(errors.length, 1);
  assert.match(errors[0], /reviewed at abc, but zeebe-pin.json pins new/);
  assert.deepEqual(evaluate(s, { reviewedAt: 'new', rules }, fs, ['intent:Job:CREATED'], bumped).errors, []);
});

test('the gap ratchet: a new cell cannot hide under a wildcard gap, and the baseline only shrinks', () => {
  const rules = [{ match: 'element:Task', ...tested }, { match: 'intent:*', ...gap }];
  const ids = ['element:Task', 'intent:Job:CREATED', 'intent:Job:NEW'];
  // A bump adds intent:Job:NEW: the wildcard claims it, but it is not in the baseline.
  assert.deepEqual(check(ids, rules, ['intent:Job:CREATED']), [
    'new gap cell: intent:Job:NEW is not in the gaps.json baseline; give it evidence or an out-of-scope rule',
  ]);
  // A baseline cell that gained evidence, or left the surface, must be dropped.
  assert.match(check(ids, rules, ['element:Task', 'intent:Job:CREATED', 'intent:Job:NEW'])[0], /lists element:Task, now nano-tested/);
  assert.match(check(ids.slice(0, 2), rules, ['intent:Job:CREATED', 'intent:Job:NEW'])[0], /lists intent:Job:NEW, which is no longer a mapped cell/);
  assert.match(check(ids, rules, ['intent:Job:NEW', 'intent:Job:CREATED'])[0], /sorted and unique/);
  assert.deepEqual(evaluate(surface('element:Task'), { rules: [{ match: '*', ...tested }] }, fs, undefined).errors, ['gaps.json must have a cells array']);
  // --update-gaps shrinks the baseline but never grows it.
  const { assignments } = evaluate(surface(...ids), { rules }, fs, []);
  assert.deepEqual(shrinkGaps(assignments, ['element:Task', 'intent:Job:CREATED', 'intent:Job:GONE']), ['intent:Job:CREATED']);
});

test('a wildcard rule can never claim a per-site failure:/rejection: cell', () => {
  const ids = ['failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR', 'rejection:JobProcessor.processRecord:NOT_FOUND'];
  // A family wildcard is rejected for both per-site families…
  const errors = check(ids, [{ match: 'failure:*', ...gap }, { match: 'rejection:*', ...gap }], []);
  assert.match(errors[0], /per-site cell failure:BpmnJobBehavior\.evalTypeExp:EXTRACT_VALUE_ERROR is claimed by wildcard rule "failure:\*"/);
  assert.match(errors[1], /per-site cell rejection:JobProcessor\.processRecord:NOT_FOUND is claimed by wildcard rule "rejection:\*"/);
  // …and so is a partial wildcard that still matches a per-site cell.
  const partial = check(['failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR'], [{ match: 'failure:BpmnJobBehavior.*', ...gap }], []);
  assert.match(partial[0], /claimed by wildcard rule "failure:BpmnJobBehavior\.\*"/);
  // An exact, wildcard-free rule claims it cleanly (a baseline gap).
  assert.deepEqual(
    check(ids, [
      { match: 'failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR', ...gap },
      { match: 'rejection:JobProcessor.processRecord:NOT_FOUND', ...gap },
    ]),
    [],
  );
});

test('a new per-site site appearing on a pin bump fails the drift guard', () => {
  // The surface gained a site the committed baseline does not list. Even though
  // an exact gap rule claims it, the ratchet rejects it: it is a NEW gap.
  const ids = ['failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR', 'failure:BpmnJobBehavior.evalRetriesExp:EXTRACT_VALUE_ERROR'];
  const rules = ids.map((id) => ({ match: id, ...gap }));
  const errors = check(ids, rules, ['failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR']);
  assert.deepEqual(errors, [
    'new gap cell: failure:BpmnJobBehavior.evalRetriesExp:EXTRACT_VALUE_ERROR is not in the gaps.json baseline; give it evidence or an out-of-scope rule',
  ]);
  // And a wildcard cannot smuggle it past the ratchet either.
  const smuggled = check(ids, [{ match: 'failure:*', ...gap }], ['failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR']);
  assert.ok(smuggled.some((e) => /claimed by wildcard rule/.test(e)));
});

test('globs treat only * as a wildcard', () => {
  assert.ok(globToRegExp('validation:V:*').test('validation:V:a-b'));
  assert.ok(!globToRegExp('guard:a.b').test('guard:aXb'));
});

test('the report tallies statuses per family', () => {
  const { assignments } = evaluate(
    surface('element:Task', 'intent:Job:CREATED', 'intent:Job:COMPLETED'),
    { rules: [{ match: 'element:*', ...tested }, { match: 'intent:*', ...gap }] },
    fs,
    ['intent:Job:COMPLETED', 'intent:Job:CREATED'],
  );
  const counts = tally(assignments);
  assert.deepEqual(counts.intent, { parity: 0, 'nano-tested': 0, gap: 2, 'out-of-scope': 0 });
  const md = markdownReport(counts, surface());
  assert.match(md, /\| element \| 0 \| 1 \| 0 \| 0 \| 1 \|/);
  assert.match(md, /\| \*\*all\*\* \| \*\*0\*\* \| \*\*1\*\* \| \*\*2\*\* \| \*\*0\*\* \| \*\*3\*\* \|/);
});

test('the committed coverage.json maps the committed surface', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const root = join(here, '..', '..');
  const load = (f) => JSON.parse(readFileSync(join(here, f), 'utf8'));
  const repo = { exists: (p) => existsSync(join(root, p)), read: (p) => readFileSync(join(root, p), 'utf8') };
  const { errors } = evaluate(load('zeebe-surface.json'), load('coverage.json'), repo, load('gaps.json').cells, load('zeebe-pin.json'));
  assert.deepEqual(errors, []);
});

test('the baseline may only shrink against the target branch', () => {
  assert.deepEqual(baselineGrowth(['a', 'b'], ['a']), []);
  assert.deepEqual(baselineGrowth(['a'], ['a', 'c']), [
    "gaps.json adds c, which the target branch's baseline does not list: the baseline may only shrink",
  ]);
  assert.deepEqual(baselineGrowth(undefined, ['a']), ['base gaps.json must have a cells array']);
});

// The pin-aware ratchet (decision A', #1396): a same-pin grammar *refinement* may
// re-baseline, but a pin *bump* and any non-refinement growth still fail.
test('the ratchet is pin-aware and permits only refinements', () => {
  const PIN_A = { repository: 'r', ref: 'stable/x', sha: 'aaa' };
  const PIN_B = { ...PIN_A, sha: 'bbb' }; // a bump
  const coarse = 'rejection:JobProcessor:NOT_FOUND';
  const refined = 'rejection:JobProcessor.processRecord:NOT_FOUND';
  const unrelated = 'intent:Job:BRAND_NEW';

  // (a) A pin bump keeps the strict shrink-only check: even a refinement fails.
  assert.deepEqual(baselineGrowth([coarse], [refined], PIN_A, PIN_B), [
    `gaps.json adds ${refined}, which the target branch's baseline does not list: the baseline may only shrink`,
  ]);

  // (b) Same pin: a refinement whose coarse parent was a base gap and is removed passes.
  assert.deepEqual(baselineGrowth([coarse], [refined], PIN_A, PIN_A), []);

  // (c) Same pin: a refinement whose parent is NOT removed (still a head gap) fails.
  assert.match(baselineGrowth([coarse], [coarse, refined], PIN_A, PIN_A)[0], new RegExp(`adds ${refined.replace('.', '\\.')}`));

  // (d) Same pin: an unrelated new cell (no coarse parent in the base) fails.
  assert.match(baselineGrowth([coarse], [refined, unrelated], PIN_A, PIN_A)[0], /adds intent:Job:BRAND_NEW/);

  // Fail-closed: no base pin means we cannot tell a refinement from a bump.
  assert.match(baselineGrowth([coarse], [refined], null, PIN_A)[0], new RegExp(`adds ${refined.replace('.', '\\.')}`));
});

test('the failure: family seeds once, then is shrink-only', () => {
  const PIN_A = { repository: 'r', ref: 'stable/x', sha: 'aaa' };
  const f1 = 'failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR';
  const f2 = 'failure:BpmnJobBehavior.evalRetriesExp:EXTRACT_VALUE_ERROR';
  // (e) Same pin, base has NO failure: cells -> the family may be seeded.
  assert.deepEqual(baselineGrowth(['intent:Job:CREATED'], ['intent:Job:CREATED', f1], PIN_A, PIN_A), []);
  // Once any failure: cell is in the base, the family is shrink-only like the rest.
  assert.match(baselineGrowth([f1], [f1, f2], PIN_A, PIN_A)[0], new RegExp(`adds ${f2.replace(/\./g, '\\.')}`));
  // Removing a coarse incident: gap must not open a back door for new failure: cells.
  const g = 'failure:P.g:FORM_NOT_FOUND';
  assert.equal(
    baselineGrowth(['failure:P.f:UNKNOWN', 'incident:FORM_NOT_FOUND'], ['failure:P.f:UNKNOWN', g], PIN_A, PIN_A).length,
    1,
  );
});

test('coarseCellId maps a per-site cell to its coarse parent, from the extractor grammar', () => {
  assert.equal(coarseCellId('rejection:JobProcessor.processRecord:NOT_FOUND'), 'rejection:JobProcessor:NOT_FOUND');
  assert.equal(coarseCellId('failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR'), 'incident:EXTRACT_VALUE_ERROR');
  // A message-only failure (empty type) and a forwarded one have no incident: parent.
  assert.equal(coarseCellId('failure:DeploymentTransformer.transform:'), null);
  assert.equal(coarseCellId('failure:BpmnStreamProcessor.process:forwarded'), null);
  // Coarse cells and non-per-site families are not refinements.
  assert.equal(coarseCellId('rejection:JobProcessor:NOT_FOUND'), null);
  assert.equal(coarseCellId('intent:Job:CREATED'), null);
});
