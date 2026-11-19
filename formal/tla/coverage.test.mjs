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

const header = (name, line, found, mod = 'Toy') =>
  `<${name} line ${line}, col 1 to line ${line}, col 3 of module ${mod}>: ${found}:${found}`;
const loc = (from, to, n, depth = 0, mod = 'Toy') =>
  `  ${'|'.repeat(depth)}line ${from}, col 5 to line ${to}, col 9 of module ${mod}: ${n}`;
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
  // Line 3 (`Guard == x < 3`) is obligated too: line obligations follow every
  // reachable definition, including the pure guard helper.
  assert.deepEqual([...exp.lines.keys()].sort((a, b) => a - b), [3, 5, 6, 8, 9]);
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

test('a wrapper action (no prime of its own) is followed to the actions it calls', () => {
  const src = `---- MODULE Wrap ----
VARIABLE x
Step(n) ==
    /\\ n > 0
    /\\ x' = n
Wrap(f) == \\E n \\in {f} : Step(n)
Loop == Loop2
Loop2 == Loop
Next ==
    \\/ Wrap(1)
    \\/ Loop
====
`;
  const exp = expectations(src);
  assert.deepEqual(exp.disjuncts, ['Wrap']);
  assert.deepEqual([...exp.closures.get('Wrap')].sort(), ['Step', 'Wrap']);
  assert.ok(exp.lines.has(4) && exp.lines.has(5));
});

test('an inline Next disjunct (no named action) is dropped — specs must name it', () => {
  // SnapshotReplay's `\/ UNCHANGED vars` stutter: the gate cannot track an
  // anonymous disjunct, so the spec names it (`RecoverAsIs`). This pins the
  // drop so a future inline disjunct is a deliberate, visible choice.
  const src = `---- MODULE Inline ----
VARIABLE x
Inc ==
    /\\ x < 3
    /\\ x' = x + 1
Next ==
    \\/ Inc
    \\/ UNCHANGED x
====
`;
  const exp = expectations(src);
  assert.deepEqual(exp.disjuncts, ['Inc']);
  // The named form IS tracked:
  const named = src.replace('Next ==\n    \\/ Inc\n    \\/ UNCHANGED x',
    'Stutter == UNCHANGED x\nNext ==\n    \\/ Inc\n    \\/ Stutter');
  assert.deepEqual(expectations(named).disjuncts, ['Inc', 'Stutter']);
});

test('line obligations include pure guard helpers reachable from an action', () => {
  const src = `---- MODULE Guarded ----
VARIABLE x
Ready ==
    \\/ x > 10
    \\/ x < 0
Inc ==
    /\\ Ready
    /\\ x' = x + 1
Next ==
    \\/ Inc
====
`;
  const exp = expectations(src);
  // Ready's disjuncts (lines 4-5) are obligations even though Ready is not an
  // action; the firing closure stays action-only.
  assert.deepEqual([...exp.lines.keys()].sort((a, b) => a - b), [4, 5, 7, 8]);
  const l = log(header('Inc', 6, 2, 'Guarded'), loc(7, 8, 2, 0, 'Guarded'));
  const problems = checkSpec({ name: 'Guarded', src, logs: [['M', l]] });
  assert.ok(problems.some((p) => p.startsWith('line 4 ')));
  assert.ok(problems.some((p) => p.startsWith('line 5 ')));
  // `/\\ Ready` (line 7) is a bare reference: TLC attributes it to Ready's
  // definition (lines 4-5), so covering 4-5 + the primed line satisfies Inc.
  const covered = log(header('Inc', 6, 2, 'Guarded'), loc(4, 5, 1, 0, 'Guarded'), loc(8, 8, 2, 0, 'Guarded'));
  assert.deepEqual(checkSpec({ name: 'Guarded', src, logs: [['M', covered]] }), []);
});
