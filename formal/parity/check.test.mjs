// Tests for the coverage guard (#1245): one test per failure mode it must catch.
import assert from 'node:assert/strict';
import { existsSync, readFileSync, readdirSync } from 'node:fs';
import { dirname, join } from 'node:path';
import { test } from 'node:test';
import { fileURLToPath } from 'node:url';

import {
  CORPUS,
  TRACES,
  baselineGrowth,
  evaluate,
  globToRegExp,
  keptIntents,
  loadTouchedCells,
  markdownReport,
  nanoTestedGrowth,
  shrinkGaps,
  shrinkNanoTested,
  tally,
} from './check.mjs';
import { coarseCellId } from './extract.mjs';

const RS = 'engine-core/src/engine/tests/lifecycle.rs';
const FILES = {
  [RS]: '// zeebe-cells: element:Task\n#[test]\nfn runs_a_task() {}\n\n#[test]\n#[should_panic]\n// zeebe-cells: element:Gone\nfn stale() {}\n\n// zeebe-cells: element:Task\n#[test]\n#[ignore = "slow"]\nfn skipped() {}\n\n#[test]\nfn bare() {}\n\n// zeebe-cells: element:Task\nfn helper() {}\n',
  [`${CORPUS}reject-x.bpmn`]: '<!-- verdict: reject | category: X -->\n<!-- zeebe-cells: validation:V:m -->\n<definitions/>',
  [`${CORPUS}reject-z.bpmn`]: '<!-- verdict: reject | category: Z -->\n<definitions/>',
  [`${CORPUS}reject-stale.bpmn`]: '<!-- verdict: reject | category: Z -->\n<!-- zeebe-cells: validation:V:m validation:V:gone -->',
  [`${CORPUS}diverge-y.bpmn`]: '<!-- verdict: diverge | category: Y -->\n<definitions/>',
  // The trace fixtures every test shares: goldens/PIN pins the capture revision
  // ('abc', the test pin) and each cells file is one captured scenario.
  [`${TRACES}goldens/PIN`]: 'abc\n',
  [`${TRACES}scenarios/s1/script.json`]: '{"steps":[]}',
  [`${TRACES}goldens/s1.trace.json`]: '[]',
  [`${TRACES}goldens/s1.cells.json`]: '{"scenario":"s1","cells":["element:Task","intent:Job:CREATED","validation:V:m"]}',
  [`${TRACES}scenarios/s2/script.json`]: '{"steps":[]}',
  [`${TRACES}goldens/s2.trace.json`]: '[]',
  [`${TRACES}goldens/s2.cells.json`]: '{"scenario":"s2","cells":["element:Other"]}',
};
const DIRS = {
  [`${TRACES}goldens`]: ['PIN', 's1.cells.json', 's1.trace.json', 's2.cells.json', 's2.trace.json'],
};
const fs = { exists: (p) => p in FILES, read: (p) => FILES[p], list: (p) => DIRS[p] ?? [] };
const PIN = { repository: 'r', ref: 'stable/x', sha: 'abc' };
// The trace fixtures above touch these, so every surface includes them: the
// grammar-drift guard rejects a trace cell the surface does not know.
const TRACE_CELLS = ['element:Task', 'intent:Job:CREATED', 'validation:V:m', 'element:Other'];
const surface = (...ids) => ({
  zeebe: { ...PIN },
  cells: [...new Set([...ids, ...TRACE_CELLS])].map((id) => ({ id, sources: ['s'] })),
});
// Unless a test is about the ratchet, the baselines are exactly the cells the
// gap / nano-tested rules claim.
const gapsOf = (ids, rules) =>
  ids.filter((id) => rules.find((r) => typeof r?.match === 'string' && globToRegExp(r.match).test(id))?.status === 'gap').sort();
const nanoTestedOf = (ids, rules) =>
  ids.filter((id) => rules.find((r) => typeof r?.match === 'string' && globToRegExp(r.match).test(id))?.status === 'nano-tested').sort();
const ALL = (ids) => [...new Set([...ids, ...TRACE_CELLS])];
// Every test surface is fully mapped: the caller's rules plus one exact gap
// rule per trace cell the caller's rules do not already claim (appended last,
// so the first-match-wins order never shadows the test's own rules).
const rulesOf = (rules) => {
  const claimed = new Set(rules.flatMap((r) => TRACE_CELLS.filter((c) => typeof r?.match === 'string' && globToRegExp(r.match).test(c))));
  const rest = TRACE_CELLS.filter((c) => !claimed.has(c)).map((c) => ({ match: c, status: 'gap', issue: 1, note: 'n' }));
  return [...rules, ...rest];
};
const check = (ids, rules, gaps, nanoTested) => {
  const all = ALL(ids);
  const full = rulesOf(rules);
  const g = gaps ?? gapsOf(all, full);
  const n = nanoTested ?? nanoTestedOf(all, full);
  return evaluate(surface(...ids), { reviewedAt: 'abc', rules: full }, fs, g, PIN, n).errors;
};

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
    'parity cell validation:V:n is not touched by any captured Zeebe trace (formal/traces/goldens/*.cells.json)',
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
  assert.deepEqual(evaluate(surface('element:Task'), {}, fs, []).errors, [
    'coverage.json must have a rules array',
    ...['element:Task', 'intent:Job:CREATED', 'validation:V:m', 'element:Other'].map((c) => `unmapped cell: ${c}`),
  ]);
});

test('a surface extracted from another Zeebe repository, ref or revision fails', () => {
  for (const k of ['repository', 'ref', 'sha']) {
    const errors = evaluate(surface('element:Task'), { reviewedAt: 'abc', rules: [{ match: '*', ...gap }] }, fs, ['element:Task'], { ...PIN, [k]: 'other' }).errors;
    assert.match(errors[0], /regenerate it$/, k);
  }
});

test('a pin bump fails until coverage.json is reviewed at the new pin and the goldens refreshed', () => {
  const rules = [{ match: 'intent:*', ...gap }, { match: 'element:*', ...gap }, { match: 'validation:*', ...gap }];
  const baseline = ['intent:Job:CREATED', 'element:Task', 'element:Other', 'validation:V:m'].sort();
  const bumped = { ...PIN, sha: 'new' };
  const s = { zeebe: bumped, cells: baseline.map((id) => ({ id, sources: ['s'] })) };
  const errors = evaluate(s, { reviewedAt: 'abc', rules }, fs, baseline, bumped).errors;
  assert.deepEqual(errors.length, 2);
  assert.match(errors[0], /reviewed at abc, but zeebe-pin.json pins new/);
  assert.match(errors[1], /trace goldens were captured at abc, but zeebe-pin.json pins new/);
  // Reviewed, and the goldens refreshed to the new pin.
  const bumpedFs = { ...fs, read: (p) => (p === `${TRACES}goldens/PIN` ? 'new\n' : FILES[p]) };
  assert.deepEqual(evaluate(s, { reviewedAt: 'new', rules }, bumpedFs, baseline, bumped).errors, []);
});

test('the gap ratchet: a new cell cannot hide under a wildcard gap, and the baseline only shrinks', () => {
  const rules = [{ match: 'element:Task', ...tested }, { match: 'intent:*', ...gap }];
  const ids = ['element:Task', 'intent:Job:CREATED', 'intent:Job:NEW'];
  // The auto-added exact gap rules for the trace cells the test does not map
  // (kept sorted: the ratchet rejects an unsorted baseline).
  const filler = ['element:Other', 'validation:V:m'];
  // A bump adds intent:Job:NEW: the wildcard claims it, but it is not in the baseline.
  assert.deepEqual(check(ids, rules, ['intent:Job:CREATED', ...filler].sort()), [
    'new gap cell: intent:Job:NEW is not in the gaps.json baseline; give it evidence or an out-of-scope rule',
  ]);
  // A baseline cell that gained evidence, or left the surface, must be dropped.
  assert.match(
    check(ids, rules, ['element:Task', 'intent:Job:CREATED', 'intent:Job:NEW', ...filler].sort())[0],
    /lists element:Task, now nano-tested/,
  );
  assert.match(
    check(ids.slice(0, 2), rules, ['intent:Job:CREATED', 'intent:Job:NEW', ...filler].sort())[0],
    /lists intent:Job:NEW, which is no longer a mapped cell/,
  );
  assert.match(check(ids, rules, ['intent:Job:NEW', 'intent:Job:CREATED', ...filler])[0], /sorted and unique/);
  assert.deepEqual(evaluate(surface('element:Task'), { rules: [{ match: '*', ...tested }] }, fs, undefined).errors, [
    ...['intent:Job:CREATED', 'validation:V:m', 'element:Other'].map((c) => `rule "*": no evidence declares ${c}`),
    'gaps.json must have a cells array',
  ]);
  // --update-gaps regenerates the baseline from the current gap assignments:
  // it shrinks away promoted/removed cells and admits newly-gapped ones (a cell
  // may legitimately regress to gap; CI's --base-gaps check keeps the baseline
  // shrink-only across branches).
  const { assignments } = evaluate(surface(...ids), { rules }, fs, []);
  assert.deepEqual(shrinkGaps(assignments, ['element:Task', 'intent:Job:CREATED', 'intent:Job:GONE']), [
    'intent:Job:CREATED',
    'intent:Job:NEW',
  ]);
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
  // The baseline also lists the auto-added exact gap rules for the trace cells (sorted).
  const baseline = ['failure:BpmnJobBehavior.evalTypeExp:EXTRACT_VALUE_ERROR', ...TRACE_CELLS].sort();
  const errors = check(ids, rules, baseline);
  assert.deepEqual(errors, [
    'new gap cell: failure:BpmnJobBehavior.evalRetriesExp:EXTRACT_VALUE_ERROR is not in the gaps.json baseline; give it evidence or an out-of-scope rule',
  ]);
  // And a wildcard cannot smuggle it past the ratchet either.
  const smuggled = check(ids, [{ match: 'failure:*', ...gap }], baseline);
  assert.ok(smuggled.some((e) => /claimed by wildcard rule/.test(e)));
});

test('parity evidence may be a captured Zeebe trace (traces:<scenario>)', () => {
  const rule = (ref) => [{ match: 'element:Task', status: 'parity', evidence: [ref] }];
  // A trace whose cells file touches the claimed cell is parity evidence.
  assert.deepEqual(check(['element:Task'], rule('traces:s1')), []);
  // The scenario, its golden trace and its cells file must all exist.
  assert.match(check(['element:Task'], rule('traces:absent'))[0], /missing scenario: traces:absent/);
  const noTrace = {
    ...fs,
    exists: (p) => p in FILES && p !== `${TRACES}goldens/s1.trace.json`,
  };
  assert.match(
    evaluate(surface('element:Task'), { reviewedAt: 'abc', rules: rule('traces:s1') }, noTrace, [], PIN, []).errors[0],
    /missing golden trace for traces:s1/,
  );
  // A scenario name becomes a file path: traversal is rejected.
  assert.match(check(['element:Task'], rule('traces:../gaps'))[0], /bad scenario name/);
  // A trace that does not touch the claimed cell declares nothing for it (s2
  // touches element:Other, so its declaration is non-empty but irrelevant).
  assert.deepEqual(check(['element:Task'], rule('traces:s2')), ['rule "element:Task": no evidence declares element:Task']);
});

test('a parity cell must be touched by a captured Zeebe trace', () => {
  // element:Other is touched by trace s2 but the rule's evidence is s1: the
  // claim needs evidence that declares it AND a trace that touches it.
  const errors = check(['element:Other'], [{ match: 'element:Other', status: 'parity', evidence: ['traces:s2'] }]);
  assert.deepEqual(errors, []);
  // A cell no committed trace touches can never be parity, whatever the evidence.
  const ids = ['element:Task', 'element:Other'];
  const untouched = check(ids, [
    { match: 'element:Task', status: 'parity', evidence: ['traces:s1'] },
    { match: 'element:Other', status: 'parity', evidence: [`${CORPUS}reject-x.bpmn`] },
  ]);
  // reject-x declares validation:V:m, not element:Other -> undeclared AND untouched.
  assert.ok(untouched.some((e) => /no evidence declares element:Other/.test(e)));
});

test('a fixture verdict vouches for validation: cells only — never for an untraced runtime cell', () => {
  // A valid fixture (accept/reject verdict) declaring a known but untraced
  // non-validation cell does NOT satisfy the trace requirement: only the
  // deploy-validation family may rest on a verdict alone.
  const fixture = `${CORPUS}reject-element.bpmn`;
  const files = { ...FILES, [fixture]: '<!-- verdict: reject | category: X -->\n<!-- zeebe-cells: element:New -->\n<definitions/>' };
  const fxs = { ...fs, exists: (p) => p in files, read: (p) => files[p] };
  const ids = ['element:New', 'validation:V:m'];
  const rules = [
    { match: 'element:New', status: 'parity', evidence: [fixture] },
    { match: 'validation:V:m', status: 'parity', evidence: [`${CORPUS}reject-x.bpmn`] },
  ];
  const errors = evaluate(surface(...ids), { reviewedAt: 'abc', rules: rulesOf(rules) }, fxs, gapsOf(ALL(ids), rulesOf(rules)), PIN, []).errors;
  assert.deepEqual(errors, [
    'parity cell element:New is not touched by any captured Zeebe trace (formal/traces/goldens/*.cells.json)',
  ]);
  // The same fixture's validation: claim passes without a trace touch…
  assert.deepEqual(check(['validation:V:m'], [rules[1]]), []);
  // …but a validation: cell no fixture declares is still rejected.
  const undeclared = check(['validation:V:n'], [{ match: 'validation:V:n', status: 'parity', evidence: [`${CORPUS}reject-x.bpmn`] }]);
  assert.ok(undeclared.some((e) => /no evidence declares validation:V:n/.test(e)));
  assert.ok(undeclared.some((e) => /validation:V:n is not touched by any captured Zeebe trace/.test(e)));
});

test('an intent the normaliser drops is not parity evidence, even when a trace records it', () => {
  // s1 touches intent:Job:CREATED; a normaliser whose JOB lane keeps only
  // COMPLETED drops it before comparison, so the differential runner never
  // checks it and the cell cannot be parity.
  const norm = { lanes: [{ valueType: 'JOB', intents: ['COMPLETED'] }] };
  const rules = [{ match: 'intent:Job:CREATED', status: 'parity', evidence: ['traces:s1'] }];
  const gaps = gapsOf(ALL(['intent:Job:CREATED']), rulesOf(rules));
  const run = (normaliser) =>
    evaluate(surface('intent:Job:CREATED'), { reviewedAt: 'abc', rules: rulesOf(rules) }, fs, gaps, PIN, [], normaliser).errors;
  assert.deepEqual(run(norm), [
    'parity cell intent:Job:CREATED is not touched by any captured Zeebe trace (formal/traces/goldens/*.cells.json)',
  ]);
  // A lane that keeps it makes the same claim pass.
  assert.deepEqual(run({ lanes: [{ valueType: 'JOB', intents: ['CREATED'] }] }), []);
  // The lane valueType maps to the cell's record stem in PascalCase.
  assert.deepEqual(keptIntents({ lanes: [{ valueType: 'PROCESS_MESSAGE_SUBSCRIPTION', intents: ['CREATED'] }] }), new Set(['intent:ProcessMessageSubscription:CREATED']));
  // A malformed spec fails loudly rather than silently dropping every intent.
  assert.ok(run({ lanes: [{ valueType: 'JOB' }] }).some((e) => /normaliser\.json must have a lanes array/.test(e)));
});

test('a cell id in a cells file that extract.mjs does not know fails (grammar drift)', () => {
  // s2's cells file lists element:Other; a surface without it drifts from the harness.
  const s = { zeebe: { ...PIN }, cells: ['element:Task', 'intent:Job:CREATED', 'validation:V:m'].map((id) => ({ id, sources: ['s'] })) };
  const rules = [
    { match: 'element:Task', status: 'parity', evidence: ['traces:s1'] },
    { match: 'intent:*', ...gap },
    { match: 'validation:*', ...gap },
  ];
  const errors = evaluate(s, { reviewedAt: 'abc', rules }, fs, ['intent:Job:CREATED', 'validation:V:m'], PIN, []).errors;
  assert.ok(errors.some((e) => e === 'formal/traces/goldens/s2.cells.json lists a cell extract.mjs does not know: element:Other'));
  // loadTouchedCells reports the same drift standalone.
  const { touched, errors: drift } = loadTouchedCells(fs, new Set(['element:Task']), 'abc');
  assert.deepEqual(drift.sort(), [
    'formal/traces/goldens/s1.cells.json lists a cell extract.mjs does not know: intent:Job:CREATED',
    'formal/traces/goldens/s1.cells.json lists a cell extract.mjs does not know: validation:V:m',
    'formal/traces/goldens/s2.cells.json lists a cell extract.mjs does not know: element:Other',
  ]);
  assert.ok(touched.has('element:Task'));
});

test('the nano-tested ratchet: the baseline must match the assignments exactly', () => {
  const rules = [{ match: 'element:Task', ...tested }, { match: 'intent:*', ...gap }];
  const ids = ['element:Task', 'intent:Job:CREATED'];
  // A nano-tested cell missing from the baseline fails…
  assert.deepEqual(check(ids, rules, gapsOf(ALL(ids), rulesOf(rules)), []), [
    'new nano-tested cell: element:Task is not in the nano-tested.json baseline; promote it to parity (a captured trace) or run check.mjs --update-ratchet',
  ]);
  // …and a baseline cell that was promoted (or left the surface) must be dropped.
  const { assignments } = evaluate(surface(...ids), { rules: rulesOf(rules) }, fs, gapsOf(ALL(ids), rulesOf(rules)), PIN);
  assert.deepEqual(shrinkNanoTested(assignments, ['element:Task', 'element:Gone']), ['element:Task']);
});

test('the nano-tested baseline may only shrink against the target branch (same pin)', () => {
  const PIN_A = { repository: 'r', ref: 'stable/x', sha: 'aaa' };
  const PIN_B = { ...PIN_A, sha: 'bbb' };
  assert.deepEqual(nanoTestedGrowth(['a', 'b'], ['a'], PIN_A, PIN_A), []);
  assert.deepEqual(nanoTestedGrowth(undefined, ['a'], PIN_A, PIN_A), ['base nano-tested.json must have a cells array']);
  // Same pin: growth fails — nano-tested is transitional, promote to parity instead.
  assert.match(nanoTestedGrowth(['a'], ['a', 'c'], PIN_A, PIN_A)[0], /nano-tested\.json adds c.*may only shrink/);
  // A pin bump may add cells (new surface cells start nano-tested while traces are captured).
  assert.deepEqual(nanoTestedGrowth(['a'], ['a', 'c'], PIN_A, PIN_B, ['a', 'b']), []);
  // Fail-closed: no base pin reads as a bump (and a bump without the base
  // surface exempts nothing — see the next test).
  assert.deepEqual(nanoTestedGrowth(['a'], ['a', 'c'], null, PIN_A, ['a', 'b']), []);
});

test('a pin bump exempts only brand-new surface cells from the nano-tested ratchet', () => {
  const PIN_A = { repository: 'r', ref: 'stable/x', sha: 'aaa' };
  const PIN_B = { ...PIN_A, sha: 'bbb' };
  const BASE_SURFACE = ['element:ServiceTask', 'intent:Job:CREATED'];
  // A cell the base surface already lists may NOT (re-)enter the baseline on a
  // pin bump: that is an existing cell regressing from parity to nano-tested.
  assert.match(
    nanoTestedGrowth(['intent:Job:CREATED'], ['intent:Job:CREATED', 'element:ServiceTask'], PIN_A, PIN_B, BASE_SURFACE)[0],
    /adds element:ServiceTask.*base surface already lists this one/,
  );
  // A brand-new cell the bump introduced (absent from the base surface) may.
  assert.deepEqual(
    nanoTestedGrowth(['intent:Job:CREATED'], ['intent:Job:CREATED', 'element:BrandNew'], PIN_A, PIN_B, BASE_SURFACE),
    [],
  );
  // Without the base surface no addition is exempt (fail-closed).
  assert.match(nanoTestedGrowth(['a'], ['a', 'c'], PIN_A, PIN_B)[0], /adds c/);
  // Same pin keeps the strict check regardless of the base surface.
  assert.match(nanoTestedGrowth(['a'], ['a', 'c'], PIN_A, PIN_A, ['x'])[0], /adds c/);
});

test('globs treat only * as a wildcard', () => {
  assert.ok(globToRegExp('validation:V:*').test('validation:V:a-b'));
  assert.ok(!globToRegExp('guard:a.b').test('guard:aXb'));
});

test('the report tallies statuses per family', () => {
  const { assignments } = evaluate(
    surface('element:Task', 'intent:Job:CREATED', 'intent:Job:COMPLETED'),
    { rules: [{ match: 'element:*', ...tested }, { match: 'intent:*', ...gap }, { match: 'validation:*', ...gap }] },
    fs,
    ['intent:Job:COMPLETED', 'intent:Job:CREATED', 'validation:V:m'],
  );
  const counts = tally(assignments);
  assert.deepEqual(counts.intent, { parity: 0, 'nano-tested': 0, gap: 2, 'out-of-scope': 0 });
  const md = markdownReport(counts, surface());
  assert.match(md, /\| element \| 0 \| 2 \| 0 \| 0 \| 2 \|/);
  assert.match(md, /\| \*\*all\*\* \| \*\*0\*\* \| \*\*2\*\* \| \*\*3\*\* \| \*\*0\*\* \| \*\*5\*\* \|/);
});

test('the committed coverage.json maps the committed surface', () => {
  const here = dirname(fileURLToPath(import.meta.url));
  const root = join(here, '..', '..');
  const load = (f) => JSON.parse(readFileSync(join(here, f), 'utf8'));
  const repo = {
    exists: (p) => existsSync(join(root, p)),
    read: (p) => readFileSync(join(root, p), 'utf8'),
    list: (p) => (existsSync(join(root, p)) ? readdirSync(join(root, p)) : []),
  };
  const { errors } = evaluate(
    load('zeebe-surface.json'),
    load('coverage.json'),
    repo,
    load('gaps.json').cells,
    load('zeebe-pin.json'),
    load('nano-tested.json').cells,
  );
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
  const SURF_NO_FAILURE = ['intent:Job:CREATED', 'incident:EXTRACT_VALUE_ERROR'];
  // (e) Same pin, base SURFACE has no failure: cells -> the family may be seeded.
  assert.deepEqual(baselineGrowth(['intent:Job:CREATED'], ['intent:Job:CREATED', f1], PIN_A, PIN_A, SURF_NO_FAILURE), []);
  // Without the base surface the seed is refused (fail-closed).
  assert.equal(baselineGrowth(['intent:Job:CREATED'], ['intent:Job:CREATED', f1], PIN_A, PIN_A).length, 1);
  // A fully covered family (empty failure: baseline, but present in the base
  // surface) must not reopen: regressing f1 back to a gap is rejected.
  assert.match(
    baselineGrowth(['intent:Job:CREATED'], ['intent:Job:CREATED', f1], PIN_A, PIN_A, [...SURF_NO_FAILURE, f1])[0],
    new RegExp(`adds ${f1.replace(/\./g, '\\.')}`),
  );
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

test('an orphan cells file (no scenario or trace) is rejected and contributes no cells', () => {
  const files = { ...FILES, [`${TRACES}goldens/orph.cells.json`]: '{"scenario":"orph","cells":["element:Orph"]}' };
  const ofs = { exists: (p) => p in files, read: (p) => files[p], list: (p) => (p === `${TRACES}goldens` ? [...DIRS[p], 'orph.cells.json'] : DIRS[p] ?? []) };
  const { touched, errors } = loadTouchedCells(ofs, undefined, 'abc');
  assert.ok(!touched.has('element:Orph'));
  assert.ok(errors.some((e) => e.includes('orphan cells file')), errors.join('\n'));
});

test('an allow-listed scenario is never parity evidence', () => {
  const files = { ...FILES, [`${TRACES}allowlist.json`]: '{"entries":[{"scenario":"s2","issue":"#1","reason":"x"}]}' };
  const afs = { exists: (p) => p in files, read: (p) => files[p], list: (p) => DIRS[p] ?? [] };
  const { touched, errors } = loadTouchedCells(afs, undefined, 'abc');
  assert.deepEqual(errors, []);
  assert.ok(!touched.has('element:Other'));
  assert.ok(touched.has('element:Task'));
});

test('an allow-listed scenario still has its cell grammar validated', () => {
  const files = {
    ...FILES,
    [`${TRACES}allowlist.json`]: '{"entries":[{"scenario":"s2","issue":"#1","reason":"x"}]}',
  };
  const afs = { exists: (p) => p in files, read: (p) => files[p], list: (p) => DIRS[p] ?? [] };
  const { touched, errors } = loadTouchedCells(afs, new Set(['element:Task']), 'abc');
  assert.ok(!touched.has('element:Other'));
  assert.ok(errors.some((e) => e.includes('does not know: element:Other')), errors.join('\n'));
});
