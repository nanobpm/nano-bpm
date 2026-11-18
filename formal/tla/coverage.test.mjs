// Tests for the TLC action-coverage gate (#1248): the expected obligations are
// derived from the spec source, so an action TLC never mentions (its
// quantifier ranges over an empty set) is still reported as a gap.
import test from 'node:test';
import assert from 'node:assert/strict';
import { checkSpec, expectations, parseCoverage, stripComments } from './coverage.mjs';

const SPEC = `---- MODULE Toy ----
VARIABLE x
Guard == x < 3
Inc ==
    /\\ Guard
    /\\ x' = x + 1
Never(n) ==
    /\\ n > 0
    /\\ x' = 0
(* a block comment
   spanning lines *)
Next ==
    \\/ Inc
    \\/ \\E n \\in {} : Never(n)
====
`;

const header = (name, line, found) =>
  `<${name} line ${line}, col 1 to line ${line}, col 3 of module Toy>: ${found}:${found}`;
const loc = (from, to, n, depth = 0) =>
  `  ${'|'.repeat(depth)}line ${from}, col 5 to line ${to}, col 9 of module Toy: ${n}`;
const log = (...rows) => ['The coverage statistics at now', ...rows, 'End of statistics.'].join('\n');

test('stripComments keeps line structure', () => {
  const out = stripComments('a \\* c\n(* x\n y *) b\n');
  assert.equal(out.split('\n').length, 4);
  assert.match(out, /^a /);
  assert.match(out.split('\n')[2], /b$/);
});

test('expectations derive Next disjuncts and action lines', () => {
  const exp = expectations(SPEC);
  assert.deepEqual(exp.disjuncts, ['Inc', 'Never']);
  assert.deepEqual([...exp.lines.keys()].sort((a, b) => a - b), [5, 6, 8, 9]);
  // `/\\ Guard` is a bare reference: TLC reports it at Guard's definition.
  assert.deepEqual(exp.alias.get(5), [3, 3]);
});

test('an action absent from the TLC report is a gap', () => {
  const l = log(header('Inc', 4, 3), loc(3, 3, 4), loc(6, 6, 3));
  const problems = checkSpec({ name: 'Toy', src: SPEC, logs: [['M1', l]] });
  assert.ok(problems.includes('action Never never fires in any model'));
  assert.ok(problems.some((p) => p.startsWith('line 8 ')));
  assert.ok(!problems.some((p) => p.startsWith('line 5 ') || p.startsWith('line 6 ')));
});

test('coverage aggregates over models', () => {
  const a = log(header('Inc', 4, 3), loc(3, 3, 4), loc(6, 6, 3));
  const b = log(header('Never', 7, 1), loc(8, 8, 1), loc(9, 9, 1));
  assert.deepEqual(checkSpec({ name: 'Toy', src: SPEC, logs: [['A', a], ['B', b]] }), []);
});

test('a zero-count location does not cover its line', () => {
  const a = log(header('Inc', 4, 3), loc(3, 3, 4), loc(6, 6, 0));
  const problems = checkSpec({ name: 'Toy', src: SPEC, logs: [['A', a]] });
  assert.ok(problems.some((p) => p.startsWith('line 6 ')));
});

test('a leaf multi-line location covers every line it spans; a decomposed one does not', () => {
  const leaf = parseCoverage(log(header('Inc', 4, 1), loc(5, 9, 1)), 'Toy');
  assert.deepEqual([...leaf.lines].sort(), [5, 6, 7, 8, 9]);
  const parent = parseCoverage(log(header('Inc', 4, 1), loc(5, 9, 1), loc(5, 5, 1, 1)), 'Toy');
  assert.deepEqual([...parent.lines].sort(), [5]);
});

test('a log without coverage statistics is reported', () => {
  const problems = checkSpec({ name: 'Toy', src: SPEC, logs: [['A', 'Model checking completed.']] });
  assert.ok(problems.includes('A: TLC log has no coverage statistics'));
});
